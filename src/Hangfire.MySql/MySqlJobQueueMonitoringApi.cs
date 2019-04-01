using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using Hangfire.Annotations;

namespace Hangfire.MySql
{
    internal class MySqlJobQueueMonitoringApi : IPersistentJobQueueMonitoringApi
    {
        private static readonly TimeSpan QueuesCacheTimeout = TimeSpan.FromSeconds(5);

        private readonly MySqlStorage _storage;
        private readonly object _cacheLock = new object();

        private List<string> _queuesCache = new List<string>();
        private Stopwatch _cacheUpdated;

        public MySqlJobQueueMonitoringApi([NotNull] MySqlStorage storage)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
        }

        public IEnumerable<string> GetQueues()
        {
            lock (_cacheLock)
            {
                if (_queuesCache.Count == 0 || _cacheUpdated.Elapsed > QueuesCacheTimeout)
                {
                    var result = _storage.UseConnection(null, connection =>
                    SqlRepository.GetQueues(connection, _storage.SchemaName, _storage.CommandTimeout));

                    _queuesCache = result;
                    _cacheUpdated = Stopwatch.StartNew();
                }

                return _queuesCache.ToList();
            }  
        }

        public IEnumerable<int> GetEnqueuedJobIds(string queue, int @from, int perPage)
        {
            return _storage.UseConnection(null, connection =>
            {
                // TODO: Remove cast to `int` to support `bigint`.
                return SqlRepository.GetEnqueuedJobIds(connection, _storage.SchemaName,
                    queue, from, perPage, _storage.CommandTimeout);
            });
        }

        public IEnumerable<int> GetFetchedJobIds(string queue, int @from, int perPage)
        {
            return _storage.UseConnection(null, connection =>
            {
                // TODO: Remove cast to `int` to support `bigint`.
                return SqlRepository.GetFetchedJobIds(connection, _storage.SchemaName,
                    queue, from, perPage);
            });
        }

        public EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue)
        {
            return _storage.UseConnection(null, connection =>
            {
                return SqlRepository.GetEnqueuedAndFetchedCount(connection, _storage.SchemaName, queue);
            });
        }

        private class JobIdDto
        {
            [UsedImplicitly]
            public long JobId { get; set; }
        }
    }
}