using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Storage;

namespace Hangfire.MySql
{
    internal class MySqlJobQueue : IPersistentJobQueue
    {
        // This is an optimization that helps to overcome the polling delay, when
        // both client and server reside in the same process. Everything is working
        // without this event, but it helps to reduce the delays in processing.
        internal static readonly AutoResetEvent NewItemInQueueEvent = new AutoResetEvent(true);

        private readonly MySqlStorage _storage;
        private readonly MySqlStorageOptions _options;

        public MySqlJobQueue([NotNull] MySqlStorage storage, MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _storage = storage;
            _options = options;
        }

        [NotNull]
        public IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            if (_options.SlidingInvisibilityTimeout.HasValue)
            {
                return DequeueUsingSlidingInvisibilityTimeout(queues, cancellationToken);
            }

            return DequeueUsingTransaction(queues, cancellationToken);
        }

        public void Enqueue(DbConnection connection, DbTransaction transaction, string queue, string jobId)
        {
            string enqueueJobSql =
$@"insert into `{_storage.SchemaName}`.JobQueue (JobId, Queue) values (@jobId, @queue)";

            connection.Execute(
                enqueueJobSql, 
                new { jobId = long.Parse(jobId), queue = queue }
                , transaction
                , commandTimeout: _storage.CommandTimeout);
        }

        private MySqlTimeoutJob DequeueUsingSlidingInvisibilityTimeout(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            FetchedJob fetchedJob = null;

            var fetchJobSqlTemplate = $@"
set transaction isolation level read committed
update top (1) JQ
set FetchedAt = GETUTCDATE()
output INSERTED.Id, INSERTED.JobId, INSERTED.Queue
from `{_storage.SchemaName}`.JobQueue JQ
where Queue in @queues and
(FetchedAt is null or FetchedAt < DATEADD(second, @timeout, GETUTCDATE()))";

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    _storage.UseConnection(null, connection =>
                    {
                        fetchedJob = connection
                            .Query<FetchedJob>(
                                fetchJobSqlTemplate,
                                new { queues = queues, timeout = _options.SlidingInvisibilityTimeout.Value.Negate().TotalSeconds })
                            .SingleOrDefault();
                    });

                    if (fetchedJob != null)
                    {
                        return new MySqlTimeoutJob(
                            _storage,
                            fetchedJob.Id,
                            fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                            fetchedJob.Queue);
                    }

                    WaitHandle.WaitAny(new WaitHandle[] { cancellationEvent.WaitHandle, NewItemInQueueEvent }, _options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (true);
            }
        }

        private MySqlTransactionJob DequeueUsingTransaction(string[] queues, CancellationToken cancellationToken)
        {
            FetchedJob fetchedJob = null;
            DbTransaction transaction = null;

            string fetchJobSqlTemplate =
                $@"delete top (1) JQ
output DELETED.Id, DELETED.JobId, DELETED.Queue
from `{_storage.SchemaName}`.JobQueue JQ
where Queue in @queues and (FetchedAt is null or FetchedAt < DATEADD(second, @timeout, GETUTCDATE()))";

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var connection = _storage.CreateAndOpenConnection();

                    try
                    {
                        transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                        fetchedJob = connection.Query<FetchedJob>(
                            fetchJobSqlTemplate,
#pragma warning disable 618
                        new { queues = queues, timeout = _options.InvisibilityTimeout.Negate().TotalSeconds },
#pragma warning restore 618
                        transaction,
                            commandTimeout: _storage.CommandTimeout).SingleOrDefault();

                        if (fetchedJob != null)
                        {
                            return new MySqlTransactionJob(
                                _storage,
                                connection,
                                transaction,
                                fetchedJob.JobId.ToString(CultureInfo.InvariantCulture),
                                fetchedJob.Queue);
                        }
                    }
                    finally
                    {
                        if (fetchedJob == null)
                        {
                            transaction?.Dispose();
                            transaction = null;

                            _storage.ReleaseConnection(connection);
                        }
                    }

                    WaitHandle.WaitAny(new WaitHandle[] { cancellationEvent.WaitHandle, NewItemInQueueEvent }, _options.QueuePollInterval);
                    cancellationToken.ThrowIfCancellationRequested();
                } while (true);
            }
        }

        [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
        private class FetchedJob
        {
            public long Id { get; set; }
            public long JobId { get; set; }
            public string Queue { get; set; }
        }
    }
}