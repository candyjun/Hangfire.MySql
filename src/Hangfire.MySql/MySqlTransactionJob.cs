using Hangfire.Annotations;
using Hangfire.Storage;
using System;
using System.Data;
using System.Threading;

namespace Hangfire.MySql
{
    internal class MySqlTransactionJob : IFetchedJob
    {
        // Connections to SQL Azure Database that are idle for 30 minutes 
        // or longer will be terminated. And since we are using separate
        // connection for a holding a transaction during the background
        // job processing, we'd like to prevent Resource Governor from 
        // terminating it.
        private static readonly TimeSpan KeepAliveInterval = TimeSpan.FromMinutes(1);

        private readonly MySqlStorage _storage;
        private IDbConnection _connection;
        private readonly IDbTransaction _transaction;
        private readonly Timer _timer;
        private readonly object _lockObject = new object();

        public MySqlTransactionJob(
            [NotNull] MySqlStorage storage,
            [NotNull] IDbConnection connection, 
            [NotNull] IDbTransaction transaction, 
            string jobId, 
            string queue)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _connection = connection ?? throw new ArgumentNullException(nameof(connection));
            _transaction = transaction ?? throw new ArgumentNullException(nameof(transaction));

            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));

            if (!_storage.IsExistingConnection(_connection))
            {
                _timer = new Timer(ExecuteKeepAliveQuery, null, KeepAliveInterval, KeepAliveInterval);
            }
        }

        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            lock (_lockObject)
            {
                _transaction.Commit();
            }
        }

        public void Requeue()
        {
            lock (_lockObject)
            {
                _transaction.Rollback();
            }
        }

        public void Dispose()
        {
            // Timer callback may be invoked after the Dispose method call,
            // so we are using lock to avoid unsynchronized calls.
            lock (_lockObject)
            {
                _timer?.Dispose();
                _transaction.Dispose();
                _storage.ReleaseConnection(_connection);
                _connection = null;
            }
        }

        private void ExecuteKeepAliveQuery(object obj)
        {
            lock (_lockObject)
            {
                try
                {
                    SqlRepository.ExecuteKeepAliveQuery(_connection, _transaction);
                }
                catch
                {
                    // Connection was closed. So we can't continue to send
                    // keep-alive queries. Unlike for distributed locks,
                    // there is no any caveats of having this issue for
                    // queues, because Hangfire guarantees only the "at least
                    // once" processing.
                }
            }
        }
    }
}
