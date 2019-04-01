using System;
using System.Threading;
using Hangfire.Annotations;
using Hangfire.Logging;
using Hangfire.Storage;

namespace Hangfire.MySql
{
    internal class MySqlTimeoutJob : IFetchedJob
    {
        private readonly ILog _logger = LogProvider.GetLogger(typeof(MySqlTimeoutJob));

        private readonly object _syncRoot = new object();
        private readonly MySqlStorage _storage;
        private readonly Timer _timer;
        private bool _disposed;
        private bool _removedFromQueue;
        private bool _requeued;

        public MySqlTimeoutJob(
            [NotNull] MySqlStorage storage,
            long id,
            [NotNull] string jobId,
            [NotNull] string queue)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));

            Id = id;
            JobId = jobId ?? throw new ArgumentNullException(nameof(jobId));
            Queue = queue ?? throw new ArgumentNullException(nameof(queue));

            if (storage.SlidingInvisibilityTimeout.HasValue)
            {
                var keepAliveInterval =
                    TimeSpan.FromSeconds(storage.SlidingInvisibilityTimeout.Value.TotalSeconds / 5);
                _timer = new Timer(ExecuteKeepAliveQuery, null, keepAliveInterval, keepAliveInterval);
            }
        }

        public long Id { get; }
        public string JobId { get; }
        public string Queue { get; }

        public void RemoveFromQueue()
        {
            lock (_syncRoot)
            {
                _storage.UseConnection(null, connection =>
                {
                    SqlRepository.RemoveFromQueue(connection, _storage.SchemaName, _storage.CommandTimeout, Id);
                });

                _removedFromQueue = true;
            }
        }

        public void Requeue()
        {
            lock (_syncRoot)
            {
                _storage.UseConnection(null, connection =>
                {
                    SqlRepository.Requeue(connection, _storage.SchemaName, _storage.CommandTimeout, Id);
                });

                _requeued = true;
            }
        }

        public void Dispose()
        {
            if (_disposed) return;
            _disposed = true;

            _timer?.Dispose();

            lock (_syncRoot)
            {
                if (!_removedFromQueue && !_requeued)
                {
                    Requeue();
                }
            }
        }

        private void ExecuteKeepAliveQuery(object obj)
        {
            lock (_syncRoot)
            {
                if (_requeued || _removedFromQueue) return;

                try
                {
                    _storage.UseConnection(null, connection =>
                    {
                        SqlRepository.ExecuteKeepAliveQuery(connection, _storage.SchemaName, _storage.CommandTimeout, Id);
                    });

                    _logger.Trace($"Keep-alive query for message {Id} sent");
                }
                catch (Exception ex)
                {
                    _logger.DebugException($"Unable to execute keep-alive query for message {Id}", ex);
                }
            }
        }
    }
}