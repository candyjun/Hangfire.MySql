using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.MySql.Entities;
using Hangfire.Server;
using Hangfire.Storage;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;

namespace Hangfire.MySql
{
    internal class SqlConnection : JobStorageConnection
    {
        private readonly MySqlStorage _storage;
        private readonly Dictionary<string, HashSet<Guid>> _lockedResources = new Dictionary<string, HashSet<Guid>>();

        public SqlConnection([NotNull] MySqlStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
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
            if (string.IsNullOrWhiteSpace(resource)) throw new ArgumentNullException(nameof(resource));
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
                    $"Multiple provider instances registered for queues: {string.Join(", ", queues)}. You should choose only one type of persistent queues per server instance.");
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

            var invocationData = InvocationData.Serialize(job);

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var jobId = SqlRepository.CreateExpiredJob(connection, _storage.SchemaName, _storage.CommandTimeout,
                    JobHelper.ToJson(invocationData), invocationData.Arguments, createdAt, createdAt.Add(expireIn));

                if (parameters.Count > 0)
                {
                    string insertParameterSql = SqlRepository.GetInsertParameterSql(_storage.SchemaName);

                    using (var commandBatch = new SqlCommandBatch())
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

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var jobData = SqlRepository.GetJobData(connection, _storage.SchemaName, _storage.CommandTimeout, long.Parse(id));

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

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var sqlState = SqlRepository.GetStateData(connection, _storage.SchemaName, _storage.CommandTimeout, long.Parse(jobId));
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
                SqlRepository.SetJobParameter(connection, _storage.SchemaName, _storage.CommandTimeout, long.Parse(id), name, value);
            });
        }

        public override string GetJobParameter(string id, string name)
        {
            if (id == null) throw new ArgumentNullException(nameof(id));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return _storage.UseConnection(_dedicatedConnection, connection => SqlRepository.GetJobParameter(
               connection, _storage.SchemaName, _storage.CommandTimeout,
                long.Parse(id), name));
        }

        public override HashSet<string> GetAllItemsFromSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = SqlRepository.GetAllItemsFromSet(
                    connection, _storage.SchemaName, _storage.CommandTimeout, key);

                return new HashSet<string>(result);
            });
        }

        public override string GetFirstByLowestScoreFromSet(string key, double fromScore, double toScore)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (toScore < fromScore) throw new ArgumentException("The `toScore` value must be higher or equal to the `fromScore` value.");

            return _storage.UseConnection(_dedicatedConnection, connection => SqlRepository.GetFirstByLowestScoreFromSet(
                connection, _storage.SchemaName, _storage.CommandTimeout, key, fromScore, toScore));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            var sql = SqlRepository.GetSetRangeInHashSql(_storage.SchemaName);

            var lockResourceKey = $"{_storage.SchemaName}:Hash:Lock";

            _storage.UseTransaction(_dedicatedConnection, (connection, transaction) =>
            {
                using (var commandBatch = new SqlCommandBatch())
                {
                    commandBatch.Append(
                        SqlRepository.GetLockResourceKeySql(),
                        new MySqlParameter("@resource", lockResourceKey));

                    foreach (var keyValuePair in keyValuePairs)
                    {
                        commandBatch.Append(sql,
                            new MySqlParameter("@key", key),
                            new MySqlParameter("@field", keyValuePair.Key),
                            new MySqlParameter("@value", (object) keyValuePair.Value ?? DBNull.Value));
                    }

                    commandBatch.Append(
                        SqlRepository.GetReleaseLockResourceKeySql(),
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
                var result = SqlRepository.GetAllEntriesFromHash(connection, _storage.SchemaName, _storage.CommandTimeout, key);

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
                SqlRepository.AnnounceServer(connection, _storage.SchemaName, _storage.CommandTimeout, serverId, JobHelper.ToJson(data), DateTime.UtcNow);
            });
        }

        public override void RemoveServer(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                SqlRepository.RemoveServer(connection, _storage.SchemaName, _storage.CommandTimeout, serverId);
            });
        }

        public override void Heartbeat(string serverId)
        {
            if (serverId == null) throw new ArgumentNullException(nameof(serverId));

            _storage.UseConnection(_dedicatedConnection, connection =>
            {
                SqlRepository.Heartbeat(connection, _storage.SchemaName, _storage.CommandTimeout, serverId, DateTime.UtcNow);
            });
        }

        public override int RemoveTimedOutServers(TimeSpan timeOut)
        {
            if (timeOut.Duration() != timeOut)
            {
                throw new ArgumentException("The `timeOut` value must be positive.", nameof(timeOut));
            }

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.RemoveTimedOutServers(
                connection, _storage.SchemaName, _storage.CommandTimeout, DateTime.UtcNow.Add(timeOut.Negate())));
        }

        public override long GetSetCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.GetSetCount(connection, _storage.SchemaName, _storage.CommandTimeout, key));
        }

        public override List<string> GetRangeFromSet(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.GetRangeFromSet(connection, _storage.SchemaName, _storage.CommandTimeout, key, startingFrom, endingAt));
        }

        public override TimeSpan GetSetTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = SqlRepository.GetSetTtl(connection, _storage.SchemaName, _storage.CommandTimeout, key);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override long GetCounter(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
                SqlRepository.GetCounter(connection, _storage.SchemaName, _storage.CommandTimeout, key) ?? 0);
        }

        public override long GetHashCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
                SqlRepository.GetHashCount(connection, _storage.SchemaName, _storage.CommandTimeout, key));
        }

        public override TimeSpan GetHashTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = SqlRepository.GetHashTtl(connection, _storage.SchemaName, _storage.CommandTimeout, key);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override string GetValueFromHash(string key, string name)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (name == null) throw new ArgumentNullException(nameof(name));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.GetValueFromHash(connection, _storage.SchemaName, _storage.CommandTimeout, key, name));
        }

        public override long GetListCount(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
                SqlRepository.GetListCount(connection, _storage.SchemaName, _storage.CommandTimeout, key));
        }

        public override TimeSpan GetListTtl(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            {
                var result = SqlRepository.GetListTtl(connection, _storage.SchemaName, _storage.CommandTimeout, key);
                if (!result.HasValue) return TimeSpan.FromSeconds(-1);

                return result.Value - DateTime.UtcNow;
            });
        }

        public override List<string> GetRangeFromList(string key, int startingFrom, int endingAt)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.GetRangeFromList(connection, _storage.SchemaName, _storage.CommandTimeout, key, startingFrom, endingAt));
        }

        public override List<string> GetAllItemsFromList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            return _storage.UseConnection(_dedicatedConnection, connection =>
            SqlRepository.GetAllItemsFromList(connection, _storage.SchemaName, _storage.CommandTimeout, key));
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
