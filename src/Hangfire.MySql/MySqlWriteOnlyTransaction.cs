using System;
using System.Collections.Generic;
using System.Data.Common;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.States;
using Hangfire.Storage;
using MySql.Data.MySqlClient;

namespace Hangfire.MySql
{
    internal class MySqlWriteOnlyTransaction : JobStorageTransaction
    {
        private readonly Queue<Action<DbConnection, DbTransaction>> _queueCommandQueue
            = new Queue<Action<DbConnection, DbTransaction>>();
        private readonly Queue<Tuple<string, MySqlParameter[]>> _commandQueue
            = new Queue<Tuple<string, MySqlParameter[]>>();
        private readonly Queue<Action> _afterCommitCommandQueue = new Queue<Action>();

        private readonly SortedSet<string> _lockedResources = new SortedSet<string>();
        private readonly MySqlStorage _storage;
        private readonly Func<DbConnection> _dedicatedConnectionFunc;

        public MySqlWriteOnlyTransaction([NotNull] MySqlStorage storage, Func<DbConnection> dedicatedConnectionFunc)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _dedicatedConnectionFunc = dedicatedConnectionFunc;
        }

        public override void Commit()
        {
            _storage.UseTransaction(_dedicatedConnectionFunc(), (connection, transaction) =>
            {
                using (var commandBatch = new SqlCommandBatch(preferBatching: _storage.CommandBatchMaxTimeout.HasValue))
                {
                    commandBatch.Append("set xact_abort on;set nocount on;");

                    foreach (var lockedResource in _lockedResources)
                    {
                        commandBatch.Append(
                            "exec sp_getapplock @Resource=@resource, @LockMode=N'Exclusive'",
                            new MySqlParameter("@resource", lockedResource));
                    }

                    foreach (var command in _commandQueue)
                    {
                        commandBatch.Append(command.Item1, command.Item2);
                    }

                    commandBatch.Connection = connection;
                    commandBatch.Transaction = transaction;
                    commandBatch.CommandTimeout = _storage.CommandTimeout;
                    commandBatch.CommandBatchMaxTimeout = _storage.CommandBatchMaxTimeout;

                    commandBatch.ExecuteNonQuery();

                    foreach (var queueCommand in _queueCommandQueue)
                    {
                        queueCommand(connection, transaction);
                    }
                }
            });

            foreach (var command in _afterCommitCommandQueue)
            {
                command();
            }
        }

        public override void ExpireJob(string jobId, TimeSpan expireIn)
        {
            QueueCommand(
                $@"update `{_storage.SchemaName}`.Job set ExpireAt = @expireAt where Id = @id",
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)),
                new MySqlParameter("@id", long.Parse(jobId)));
        }

        private void QueueCommand(string commandText, params MySqlParameter[] parameters)
        {
            _commandQueue.Enqueue(new Tuple<string, MySqlParameter[]>(commandText, parameters));
        }

        public override void PersistJob(string jobId)
        {
            QueueCommand(
                $@"update `{_storage.SchemaName}`.Job set ExpireAt = NULL where Id = @id",
                new MySqlParameter("@id", long.Parse(jobId)));
        }

        public override void SetJobState(string jobId, IState state)
        {
            string addAndSetStateSql = 
$@"insert into `{_storage.SchemaName}`.State (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data);
update `{_storage.SchemaName}`.Job set StateId = SCOPE_IDENTITY(), StateName = @name where Id = @id;";

            QueueCommand(addAndSetStateSql,
                new MySqlParameter("@jobId", long.Parse(jobId)),
                new MySqlParameter("@name", state.Name),
                new MySqlParameter("@reason", (object)state.Reason?.Substring(0, Math.Min(99, state.Reason.Length)) ?? DBNull.Value),
                new MySqlParameter("@createdAt", DateTime.UtcNow),
                new MySqlParameter("@data", (object)JobHelper.ToJson(state.SerializeData()) ?? DBNull.Value),
                new MySqlParameter("@id", long.Parse(jobId)));
        }

        public override void AddJobState(string jobId, IState state)
        {
            string addStateSql =
$@"insert into `{_storage.SchemaName}`.State (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data)";

            QueueCommand(addStateSql,
                new MySqlParameter("@jobId", long.Parse(jobId)),
                new MySqlParameter("@name", state.Name),
                new MySqlParameter("@reason", (object)state.Reason?.Substring(0, Math.Min(99, state.Reason.Length)) ?? DBNull.Value),
                new MySqlParameter("@createdAt", DateTime.UtcNow),
                new MySqlParameter("@data", (object)JobHelper.ToJson(state.SerializeData()) ?? DBNull.Value));
        }

        public override void AddToQueue(string queue, string jobId)
        {
            var provider = _storage.QueueProviders.GetProvider(queue);
            var persistentQueue = provider.GetJobQueue();

            _queueCommandQueue.Enqueue((connection, transaction) => persistentQueue.Enqueue(
                connection,
                transaction,
                queue,
                jobId));

            if (persistentQueue.GetType() == typeof(MySqlJobQueue))
            {
                _afterCommitCommandQueue.Enqueue(() => MySqlJobQueue.NewItemInQueueEvent.Set());
            }
        }

        public override void IncrementCounter(string key)
        {
            QueueCommand(
                $@"insert into `{_storage.SchemaName}`.Counter (`Key`, `Value`) values (@key, @value)",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", +1));
        }

        public override void IncrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(
                $@"insert into `{_storage.SchemaName}`.Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", +1),
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)));
        }

        public override void DecrementCounter(string key)
        {
            QueueCommand(
                $@"insert into `{_storage.SchemaName}`.Counter (`Key`, `Value`) values (@key, @value)",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", -1));
        }

        public override void DecrementCounter(string key, TimeSpan expireIn)
        {
            QueueCommand(
                $@"insert into `{_storage.SchemaName}`.Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", -1),
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)));
        }

        public override void AddToSet(string key, string value)
        {
            AddToSet(key, value, 0.0);
        }

        public override void AddToSet(string key, string value, double score)
        {
            string addSql =
$@";merge `{_storage.SchemaName}`.`Set` as Target
using (VALUES (@key, @value, @score)) as Source (`Key`, Value, Score)
on Target.`Key` = Source.`Key` and Target.Value = Source.Value
when matched then update set Score = Source.Score
when not matched then insert (`Key`, Value, Score) values (Source.`Key`, Source.Value, Source.Score);";

            AcquireSetLock();
            QueueCommand(addSql,
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", value),
                new MySqlParameter("@score", score));
        }

        public override void RemoveFromSet(string key, string value)
        {
            string query = $@"delete from `{_storage.SchemaName}`.`Set` where `Key` = @key and Value = @value";

            AcquireSetLock();
            QueueCommand(query,
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", value));
        }

        public override void InsertToList(string key, string value)
        {
            AcquireListLock();
            QueueCommand(
                $@"insert into `{_storage.SchemaName}`.List (`Key`, Value) values (@key, @value);",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", value));
        }

        public override void RemoveFromList(string key, string value)
        {
            AcquireListLock();
            QueueCommand(
                $@"delete from `{_storage.SchemaName}`.List where `Key` = @key and Value = @value",
                new MySqlParameter("@key", key),
                new MySqlParameter("@value", value));
        }

        public override void TrimList(string key, int keepStartingFrom, int keepEndingAt)
        {
            string trimSql =
$@";with cte as (
    select row_number() over (order by Id desc) as row_num
    from `{_storage.SchemaName}`.List
    where `Key` = @key)
delete from cte where row_num not between @start and @end";

            AcquireListLock();
            QueueCommand(trimSql,
                new MySqlParameter("@key", key),
                new MySqlParameter("@start", keepStartingFrom + 1),
                new MySqlParameter("@end", keepEndingAt + 1));
        }

        public override void SetRangeInHash(string key, IEnumerable<KeyValuePair<string, string>> keyValuePairs)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (keyValuePairs == null) throw new ArgumentNullException(nameof(keyValuePairs));

            string sql =
$@";merge `{_storage.SchemaName}`.Hash as Target
using (VALUES (@key, @field, @value)) as Source (`Key`, Field, Value)
on Target.`Key` = Source.`Key` and Target.Field = Source.Field
when matched then update set Value = Source.Value
when not matched then insert (`Key`, Field, Value) values (Source.`Key`, Source.Field, Source.Value);";

            AcquireHashLock();

            foreach (var pair in keyValuePairs)
            {
                QueueCommand(sql,
                    new MySqlParameter("@key", key),
                    new MySqlParameter("@field", pair.Key),
                    new MySqlParameter("@value", (object)pair.Value ?? DBNull.Value));
            }
        }

        public override void RemoveHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"delete from `{_storage.SchemaName}`.Hash where `Key` = @key";

            AcquireHashLock();
            QueueCommand(query, new MySqlParameter("@key", key));
        }

        public override void AddRangeToSet(string key, IList<string> items)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));
            if (items == null) throw new ArgumentNullException(nameof(items));

            // TODO: Rewrite using the `MERGE` statement.
            string query =
$@"insert into `{_storage.SchemaName}`.`Set` (`Key`, Value, Score)
values (@key, @value, 0.0)";

            AcquireSetLock();

            foreach (var item in items)
            {
                QueueCommand(query, new MySqlParameter("@key", key), new MySqlParameter("@value", item));
            }
        }

        public override void RemoveSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"delete from `{_storage.SchemaName}`.`Set` where `Key` = @key";

            AcquireSetLock();
            QueueCommand(query, new MySqlParameter("@key", key));
        }

        public override void ExpireHash(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

             string query = $@"
update `{_storage.SchemaName}`.`Hash` set ExpireAt = @expireAt where `Key` = @key";

            AcquireHashLock();
            QueueCommand(query,
                new MySqlParameter("@key", key),
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)));
        }

        public override void ExpireSet(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"
update `{_storage.SchemaName}`.`Set` set ExpireAt = @expireAt where `Key` = @key";

            AcquireSetLock();
            QueueCommand(query,
                new MySqlParameter("@key", key),
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)));
        }

        public override void ExpireList(string key, TimeSpan expireIn)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"
update `{_storage.SchemaName}`.`List` set ExpireAt = @expireAt where `Key` = @key";

            AcquireListLock();
            QueueCommand(query,
                new MySqlParameter("@key", key),
                new MySqlParameter("@expireAt", DateTime.UtcNow.Add(expireIn)));
        }

        public override void PersistHash(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"
update `{_storage.SchemaName}`.Hash set ExpireAt = null where `Key` = @key";

            AcquireHashLock();
            QueueCommand(query, new MySqlParameter("@key", key));
        }

        public override void PersistSet(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"
update `{_storage.SchemaName}`.`Set` set ExpireAt = null where `Key` = @key";

            AcquireSetLock();
            QueueCommand(query, new MySqlParameter("@key", key));
        }

        public override void PersistList(string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query = $@"
update `{_storage.SchemaName}`.`List` set ExpireAt = null where `Key` = @key";

            AcquireListLock();
            QueueCommand(query, new MySqlParameter("@key", key));
        }

        private void AcquireListLock()
        {
            AcquireLock("List");
        }

        private void AcquireSetLock()
        {
            AcquireLock("Set");
        }

        private void AcquireHashLock()
        {
            AcquireLock("Hash");
        }

        private void AcquireLock(string resource)
        {
            _lockedResources.Add($"{_storage.SchemaName}:{resource}:Lock");
        }
    }
}