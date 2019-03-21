using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Server;
using Hangfire.MySql.Entities;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    internal class SqlConnection : JobStorageConnection
    {
        private readonly MySqlStorage _storage;
        private readonly Dictionary<string, HashSet<Guid>> _lockedResources = new Dictionary<string, HashSet<Guid>>();

        public SqlConnection([NotNull] MySqlStorage storage)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            _storage = storage;
        }

        public override void Dispose()
        {
            if (_dedicatedConnection != null)
            {
                _dedicatedConnection.Dispose();
                _dedicatedConnection = null;
            }
        }

        public override IWriteOnlyTransaction CreateWriteTransaction()
        {
            return new MySqlWriteOnlyTransaction(_storage, () => _dedicatedConnection);
        }

        public override IDisposable AcquireDistributedLock([NotNull] string resource, TimeSpan timeout)
        {
            if (String.IsNullOrWhiteSpace(resource)) throw new ArgumentNullException(nameof(resource));
            return AcquireLock($"{_storage.SchemaName}:{resource}", timeout);
        }

        public override IFetchedJob FetchNextJob(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null || queues.Length == 0) throw new ArgumentNullException(nameof(queues));

            var providers = queues
                .Select(queue => _storage.QueueProviders.GetProvider(queue))
                .Distinct()
                .ToArray();

            if (providers.Length != 1)
            {
                throw new InvalidOperationException(
                    $"Multiple provider instances registered for queues: {String.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
            }
            
            var persistentQueue = providers[0].GetJobQueue();
            return persistentQueue.Dequeue(queues, cancellationToken);
        }

        public override string CreateExpiredJob(
            Job job,
            IDictionary<string, string> parameters, 
            DateTime createdAt,
            TimeSpan expireIn)
        {
            if (job == null) throw new ArgumentNullException(nameof(job));
            if (parameters == null) throw new ArgumentNullException(nameof(parameters));

            string createJobSql =
$@"insert into `{_storage.SchemaName}`.Job (InvocationData, Arguments, CreatedAt, ExpireAt)
output inserted.Id
values (@invocationData, @arguments, @createdAt, @expireAt)";

            var invocationData = InvocationData.Serialize(job);

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var jobId = connection.ExecuteScalar<long>(
                    createJobSql,
                    new
                    {
                        invocationData = JobHelper.ToJson(invocationData),
                        arguments = invocationData.Arguments,
                        createdAt = createdAt,
                        expireAt = createdAt.Add(expireIn)
                    },
                    commandTimeout: _storage.CommandTimeout).ToString();

                if (parameters.Count > 0)
                {
                    string insertParameterSql =
$@"insert into `{_storage.SchemaName}`.JobParameter (JobId, Name, Value)
values (@jobId, @name, @value)";

                    using (var commandBatch = new SqlCommandBatch(preferBatching: _storage.CommandBatchMaxTimeout.HasValue))
                    {

                        foreach (var parameter in parameters)
                        {
                            commandBatch.Append(insertParameterSql,
                                new MySqlParameter("@jobId", long.Parse(jobId)),
                                new MySqlParameter("@name", parameter.Key),
                                new MySqlParameter("@value", (object)parameter.Value ?? DBNull.Value));
                        }

                        commandBatch.Connection = connection;
                        commandBatch.CommandTimeout = _storage.CommandTimeout;
                        commandBatch.CommandBatchMaxTimeout = _storage.CommandBatchMaxTimeout;

                        commandBatch.ExecuteNonQuery();
                    }
                }

                return jobId;
            });
        }

        public override JobData GetJobData(string id)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));

            string sql =
$@"select InvocationData, StateName, Arguments, CreatedAt from `{_storage.SchemaName}`.Job  where Id = @id";

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var jobData = connection.Query<SqlJob>(sql, new { id = long.Parse(id) }, commandTimeout: _storage.CommandTimeout)
                    .SingleOrDefault();

                if (jobData == null) return null;

                // TODO: conversion exception could be thrown.
                var invocationData = JobHelper.FromJson<InvocationData>(jobData.InvocationData);
                invocationData.Arguments = jobData.Arguments;

                Job job = null;
                JobLoadException loadException = null;

                try
                {
                    job = invocationData.Deserialize();
                }
                catch (JobLoadException ex)
                {
                    loadException = ex;
                }

                return new JobData
                {
                    Job = job,
                    State = jobData.StateName,
                    CreatedAt = jobData.CreatedAt,
                    LoadException = loadException
                };
            });
        }

        public override StateData GetStateData(string jobId)
        {
            if (jobId == null) throw new ArgumentNullException(nameof(jobId));

            string sql = 
$@"select s.Name, s.Reason, s.Data
from `{_storage.SchemaName}`.State s 
inner join `{_storage.SchemaName}`.Job j  on j.StateId = s.Id and j.Id = s.JobId
where j.Id = @jobId";

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var sqlState = connection.Query<SqlState>(sql, new { jobId = long.Parse(jobId) }, commandTimeout: _storage.CommandTimeout).SingleOrDefault();
                if (sqlState == null)
                {
                    return null;
                }

                var data = new Dictionary<string, string>(
                    JobHelper.FromJson<Dictionary<string, string>>(sqlState.Data),
                    StringComparer.OrdinalIgnoreCase);

                return new StateData
                {
                    Name = sqlState.Name,
                    Reason = sqlState.Reason,
                    Data = data
                };
            });
        }

        public override void SetJobParameter(string id, string name, string value)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                connection.Execute(
$@";merge `{_storage.SchemaName}`.JobParameter as Target
using (VALUES (@jobId, @name, @value)) as Source (JobId, Name, Value) 
on Target.JobId = Source.JobId AND Target.Name = Source.Name
when matched then update set Value = Source.Value
when not matched then insert (JobId, Name, Value) values (Source.JobId, Source.Name, Source.Value);",
                    new { jobId = long.Parse(id), name, value },
                    commandTimeout: _storage.CommandTimeout);
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return _storage.UseConnection(_dedicatedConnection, connection => connection.ExecuteScalar<string>(
                $@"select top (1) Value from `{_storage.SchemaName}`.JobParameter  where JobId = @id and Name = @name",
                new { id = long.Parse(id), name = name },
                commandTimeout: _storage.CommandTimeout));
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = connection.Query<string>(
                    $@"select Value from `{_storage.SchemaName}`.`Set`  where `Key` = @key",
                    new { key },
                    commandTimeout: _storage.CommandTimeout);

                return new HashSet<string>(result);
            });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _storage.UseConnection(_dedicatedConnection, connection => connection.ExecuteScalar<string>(
                $@"select top 1 Value from `{_storage.SchemaName}`.`Set`  where `Key` = @key and Score between @from and @to order by Score",
                new { key, from = fromScore, to = toScore },
                commandTimeout: _storage.CommandTimeout));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            var sql =
$@";merge `{_storage.SchemaName}`.Hash as Target
using (VALUES (@key, @field, @value)) as Source (`Key`, Field, Value)
on Target.`Key` = Source.`Key` and Target.Field = Source.Field
when matched then update set Value = Source.Value
when not matched then insert (`Key`, Field, Value) values (Source.`Key`, Source.Field, Source.Value);";

            var lockResourceKey = $"{_storage.SchemaName}:Hash:Lock";

            _storage.UseTransaction(_dedicatedConnection, (connection, transaction) =>
            {
                using (var commandBatch = new SqlCommandBatch(preferBatching: _storage.CommandBatchMaxTimeout.HasValue))
                {
                    commandBatch.Append(
                        "SET XACT_ABORT ON;exec GET_LOCK @Resource=@resource, @LockMode=N'Exclusive', @LockOwner=N'Transaction', @LockTimeout=-1;",
                        new MySqlParameter("@resource", lockResourceKey));

                    foreach (var keyValuePair in keyValuePairs)
                    {
                        commandBatch.Append(sql,
                            new MySqlParameter("@key", key),
                            new MySqlParameter("@field", keyValuePair.Key),
                            new MySqlParameter("@value", (object) keyValuePair.Value ?? DBNull.Value));
                    }

                    commandBatch.Append(
                        "exec RELEASE_LOCK @Resource=@resource, @LockOwner=N'Transaction';",
                        new MySqlParameter("@resource", lockResourceKey));

                    commandBatch.Connection = connection;
                    commandBatch.Transaction = transaction;
                    commandBatch.CommandTimeout = _storage.CommandTimeout;
                    commandBatch.CommandBatchMaxTimeout = _storage.CommandBatchMaxTimeout;

                    commandBatch.ExecuteNonQuery();
                }
            });
        }

        public override Dictionary<string, string> GetAllEntriesFromHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = connection.Query<SqlHash>(
                    $"select Field, Value from `{_storage.SchemaName}`.Hash where `Key` = @key",
                    new { key },
                    commandTimeout: _storage.CommandTimeout)
                    .ToDictionary(x => x.Field, x => x.Value);

                return result.Count != 0 ? result : null;
            });
        }

        public override void AnnounceServer(string serverId, ServerContext context)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));
            if (context == null) throw new ArgumentNullException(nameof(context));

            var data = new ServerData
            {
                WorkerCount = context.WorkerCount,
                Queues = context.Queues,
                StartedAt = DateTime.UtcNow,
            };

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                connection.Execute(
$@";merge `{_storage.SchemaName}`.Server as Target
using (VALUES (@id, @data, @heartbeat)) as Source (Id, Data, Heartbeat)
on Target.Id = Source.Id
when matched then update set Data = Source.Data, LastHeartbeat = Source.Heartbeat
when not matched then insert (Id, Data, LastHeartbeat) values (Source.Id, Source.Data, Source.Heartbeat);",
                    new { id = serverId, data = JobHelper.ToJson(data), heartbeat = DateTime.UtcNow },
                    commandTimeout: _storage.CommandTimeout);
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                connection.Execute(
                    $@"delete from `{_storage.SchemaName}`.Server where Id = @id",
                    new { id = serverId },
                    commandTimeout: _storage.CommandTimeout);
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                connection.Execute(
                    $@"update `{_storage.SchemaName}`.Server set LastHeartbeat = @now where Id = @id",
                    new { now = DateTime.UtcNow, id = serverId },
                    commandTimeout: _storage.CommandTimeout);
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return _storage.UseConnection(_dedicatedConnection, connection => connection.Execute(
                $@"delete from `{_storage.SchemaName}`.Server where LastHeartbeat < @timeOutAt",
                new { timeOutAt = DateTime.UtcNow.Add(timeOut.Negate()) },
                commandTimeout: _storage.CommandTimeout));
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection => connection.Query<int>(
                $"select count(`Key`) from `{_storage.SchemaName}`.`Set`  where `Key` = @key",
                new { key = key },
                commandTimeout: _storage.CommandTimeout).First());
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query =
$@"select `Value` from (
	select `Value`, row_number() over (order by `Key` ASC) as row_num
	from `{_storage.SchemaName}`.`Set` 
	where `Key` = @key 
) as s where s.row_num between @startingFrom and @endingAt";

            return _storage.UseConnection(_dedicatedConnection, connection => connection
                .Query<string>(query, new { key = key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 }, commandTimeout: _storage.CommandTimeout)
                .ToList());
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"select min(`ExpireAt`) from `{_storage.SchemaName}`.`Set`  where `Key` = @key";

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = connection.ExecuteScalar<DateTime?>(query, new { key = key }, commandTimeout: _storage.CommandTimeout);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"select sum(s.`Value`) from (select sum(`Value`) as `Value` from `{_storage.SchemaName}`.Counter 
where `Key` = @key
union all
select `Value` from `{_storage.SchemaName}`.AggregatedCounter 
where `Key` = @key) as s";

            return _storage.UseConnection(_dedicatedConnection, connection => 
                connection.ExecuteScalar<long?>(query, new { key = key }, commandTimeout: _storage.CommandTimeout) ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"select count(*) from `{_storage.SchemaName}`.Hash  where `Key` = @key";

            return _storage.UseConnection(_dedicatedConnection, connection => 
                connection.ExecuteScalar<long>(query, new { key = key }, commandTimeout: _storage.CommandTimeout));
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"select min(`ExpireAt`) from `{_storage.SchemaName}`.Hash  where `Key` = @key";

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = connection.ExecuteScalar<DateTime?>(query, new { key = key }, commandTimeout: _storage.CommandTimeout);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            string query =
$@"select `Value` from `{_storage.SchemaName}`.Hash 
where `Key` = @key and `Field` = @field";

            return _storage.UseConnection(_dedicatedConnection, connection => connection
                .ExecuteScalar<string>(query, new { key = key, field = name }, commandTimeout: _storage.CommandTimeout));
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"select count(`Id`) from `{_storage.SchemaName}`.List 
where `Key` = @key";

            return _storage.UseConnection(_dedicatedConnection, connection => 
                connection.ExecuteScalar<long>(query, new { key = key }, commandTimeout: _storage.CommandTimeout));
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = 
$@"select min(`ExpireAt`) from `{_storage.SchemaName}`.List 
where `Key` = @key";

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = connection.ExecuteScalar<DateTime?>(query, new { key = key }, commandTimeout: _storage.CommandTimeout);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query =
$@"select `Value` from (
	select `Value`, row_number() over (order by `Id` desc) as row_num 
	from `{_storage.SchemaName}`.List 
	where `Key` = @key 
) as s where s.row_num between @startingFrom and @endingAt";

            return _storage.UseConnection(_dedicatedConnection, connection => connection
                .Query<string>(query, new { key = key, startingFrom = startingFrom + 1, endingAt = endingAt + 1 }, commandTimeout: _storage.CommandTimeout)
                .ToList());
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query =
$@"select `Value` from `{_storage.SchemaName}`.List 
where `Key` = @key
order by `Id` desc";

            return _storage.UseConnection(_dedicatedConnection, connection => connection.Query<string>(query, new { key = key }, commandTimeout: _storage.CommandTimeout).ToList());
        }

        private DbConnection _dedicatedConnection;

        private IDisposable AcquireLock(string resource, TimeSpan timeout)
        {
            if (_dedicatedConnection == null)
            {
                _dedicatedConnection = _storage.CreateAndOpenConnection();
            }

            var lockId = Guid.NewGuid();

            if (!_lockedResources.ContainsKey(resource))
            {
                try
                {
                    MySqlDistributedLock.Acquire(_dedicatedConnection, resource, timeout);
                }
                catch (Exception)
                {
                    ReleaseLock(resource, lockId, true);
                    throw;
                }

                _lockedResources.Add(resource, new HashSet<Guid>());
            }

            _lockedResources[resource].Add(lockId);
            return new DisposableLock(this, resource, lockId);
        }

        private void ReleaseLock(string resource, Guid lockId, bool onDisposing)
        {
            try
            {
                if (_lockedResources.ContainsKey(resource))
                {
                    if (_lockedResources[resource].Contains(lockId))
                    {
                        if (_lockedResources[resource].Remove(lockId) &&
                            _lockedResources[resource].Count == 0 &&
                            _lockedResources.Remove(resource) &&
                            _dedicatedConnection.State == ConnectionState.Open)
                        {
                            // Session-scoped application locks are held only when connection
                            // is open. When connection is closed or broken, for example, when
                            // there was an error, application lock is already released by SQL
                            // Server itself, and we shouldn't do anything.
                            MySqlDistributedLock.Release(_dedicatedConnection, resource);
                        }
                    }
                }

                if (_lockedResources.Count == 0)
                {
                    _storage.ReleaseConnection(_dedicatedConnection);
                    _dedicatedConnection = null;
                }
            }
            catch (Exception)
            {
                if (!onDisposing)
                {
                    throw;
                }
            }
        }

        private class DisposableLock : IDisposable
        {
            private readonly SqlConnection _connection;
            private readonly string _resource;
            private readonly Guid _lockId;

            public DisposableLock(SqlConnection connection, string resource, Guid lockId)
            {
                _connection = connection;
                _resource = resource;
                _lockId = lockId;
            }

            public void Dispose()
            {
                _connection.ReleaseLock(_resource, _lockId, true);
            }
        }
    }
}
