using System;
using Hangfire.Annotations;

namespace Hangfire.MySql
{
    internal class MySqlJobQueueProvider : IPersistentJobQueueProvider
    {
        private readonly IPersistentJobQueue _jobQueue;
        private readonly IPersistentJobQueueMonitoringApi _monitoringApi;

        public MySqlJobQueueProvider([NotNull] MySqlStorage storage, [NotNull] MySqlStorageOptions options)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));
            if (options == null) throw new ArgumentNullException(nameof(options));

            _jobQueue = new MySqlJobQueue(storage, options);
            _monitoringApi = new MySqlJobQueueMonitoringApi(storage);
        }

        public IPersistentJobQueue GetJobQueue()
        {
            return _jobQueue;
        }

        public IPersistentJobQueueMonitoringApi GetJobQueueMonitoringApi()
        {
            return _monitoringApi;
        }
    }
}