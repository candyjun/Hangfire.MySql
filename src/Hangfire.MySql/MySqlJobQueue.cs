using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.Storage;
using System;
using System.Data;
using System.Data.Common;
using System.Globalization;
using System.Threading;

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
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _options = options ?? throw new ArgumentNullException(nameof(options));
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
            SqlRepository.AddJobQueue(connection, _storage.SchemaName, _storage.CommandTimeout, transaction, queue, jobId);
        }

        private MySqlTimeoutJob DequeueUsingSlidingInvisibilityTimeout(string[] queues, CancellationToken cancellationToken)
        {
            if (queues == null) throw new ArgumentNullException(nameof(queues));
            if (queues.Length == 0) throw new ArgumentException("Queue array must be non-empty.", nameof(queues));

            FetchedJob fetchedJob = null;

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    _storage.UseConnection(null, connection =>
                    {
                        fetchedJob = SqlRepository.GetFetchedJob<FetchedJob>(connection, _storage.SchemaName, queues,
                            _options.SlidingInvisibilityTimeout.Value.Negate().TotalSeconds);
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

            using (var cancellationEvent = cancellationToken.GetCancellationEvent())
            {
                do
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    var connection = _storage.CreateAndOpenConnection();

                    try
                    {
                        transaction = connection.BeginTransaction(IsolationLevel.ReadCommitted);

                        fetchedJob = SqlRepository.GetFetchedJobUsingTransaction<FetchedJob>(connection, _storage.SchemaName,
                            queues,
#pragma warning disable 618
                        _options.InvisibilityTimeout.Negate().TotalSeconds,
#pragma warning restore 618
                            transaction, _storage.CommandTimeout);
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