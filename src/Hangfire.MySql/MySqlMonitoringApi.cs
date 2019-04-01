using Hangfire.Annotations;
using Hangfire.Common;
using Hangfire.MySql.Entities;
using Hangfire.States;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace Hangfire.MySql
{
    internal class MySqlMonitoringApi : IMonitoringApi
    {
        private readonly MySqlStorage _storage;
        private readonly int? _jobListLimit;

        public MySqlMonitoringApi([NotNull] MySqlStorage storage, int? jobListLimit)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _jobListLimit = jobListLimit;
        }

        public long ScheduledCount()
        {
            return UseConnection(connection => 
                GetNumberOfJobsByStateName(connection, ScheduledState.StateName));
        }

        public long EnqueuedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.EnqueuedCount ?? 0;
        }

        public long FetchedCount(string queue)
        {
            var queueApi = GetQueueApi(queue);
            var counters = queueApi.GetEnqueuedAndFetchedCount(queue);

            return counters.FetchedCount ?? 0;
        }

        public long FailedCount()
        {
            return UseConnection(connection => 
                GetNumberOfJobsByStateName(connection, FailedState.StateName));
        }

        public long ProcessingCount()
        {
            return UseConnection(connection => 
                GetNumberOfJobsByStateName(connection, ProcessingState.StateName));
        }

        public JobList<ProcessingJobDto> ProcessingJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ProcessingState.StateName,
                (sqlJob, job, stateData) => new ProcessingJobDto
                {
                    Job = job,
                    InProcessingState = ProcessingState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    ServerId = stateData.ContainsKey("ServerId") ? stateData["ServerId"] : stateData["ServerName"],
                    StartedAt = JobHelper.DeserializeNullableDateTime(stateData["StartedAt"]),
                }));
        }

        public JobList<ScheduledJobDto> ScheduledJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from, count,
                ScheduledState.StateName,
                (sqlJob, job, stateData) => new ScheduledJobDto
                {
                    Job = job,
                    InScheduledState = ScheduledState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueueAt = JobHelper.DeserializeNullableDateTime(stateData["EnqueueAt"]) ?? DateTime.MinValue,
                    ScheduledAt = JobHelper.DeserializeNullableDateTime(stateData["ScheduledAt"])
                }));
        }

        public IDictionary<DateTime, long> SucceededByDatesCount()
        {
            return UseConnection(connection => 
                GetTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> FailedByDatesCount()
        {
            return UseConnection(connection => 
                GetTimelineStats(connection, "failed"));
        }

        public IList<ServerDto> Servers()
        {
            return UseConnection<IList<ServerDto>>(connection =>
            {
                var servers = SqlRepository.GetServers(connection, _storage.SchemaName, _storage.CommandTimeout);
                var result = new List<ServerDto>();

                foreach (var server in servers)
                {
                    var data = JobHelper.FromJson<ServerData>(server.Data);
                    result.Add(new ServerDto
                    {
                        Name = server.Id,
                        Heartbeat = server.LastHeartbeat,
                        Queues = data.Queues,
                        StartedAt = data.StartedAt ?? DateTime.MinValue,
                        WorkersCount = data.WorkerCount
                    });
                }

                return result;
            });
        }

        public JobList<FailedJobDto> FailedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                FailedState.StateName,
                (sqlJob, job, stateData) => new FailedJobDto
                {
                    Job = job,
                    InFailedState = FailedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Reason = sqlJob.StateReason,
                    ExceptionDetails = stateData["ExceptionDetails"],
                    ExceptionMessage = stateData["ExceptionMessage"],
                    ExceptionType = stateData["ExceptionType"],
                    FailedAt = JobHelper.DeserializeNullableDateTime(stateData["FailedAt"])
                }));
        }

        public JobList<SucceededJobDto> SucceededJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                SucceededState.StateName,
                (sqlJob, job, stateData) => new SucceededJobDto
                {
                    Job = job,
                    InSucceededState = SucceededState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    Result = stateData["Result"],
                    TotalDuration = stateData.ContainsKey("PerformanceDuration") && stateData.ContainsKey("Latency")
                        ? (long?)long.Parse(stateData["PerformanceDuration"]) + (long?)long.Parse(stateData["Latency"])
                        : null,
                    SucceededAt = JobHelper.DeserializeNullableDateTime(stateData["SucceededAt"])
                }));
        }

        public JobList<DeletedJobDto> DeletedJobs(int @from, int count)
        {
            return UseConnection(connection => GetJobs(
                connection,
                from,
                count,
                DeletedState.StateName,
                (sqlJob, job, stateData) => new DeletedJobDto
                {
                    Job = job,
                    InDeletedState = DeletedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    DeletedAt = JobHelper.DeserializeNullableDateTime(stateData["DeletedAt"])
                }));
        }

        public IList<QueueWithTopEnqueuedJobsDto> Queues()
        {
            var tuples = _storage.QueueProviders
                .Select(x => x.GetJobQueueMonitoringApi())
                .SelectMany(x => x.GetQueues(), (monitoring, queue) => new { Monitoring = monitoring, Queue = queue })
                .OrderBy(x => x.Queue)
                .ToArray();

            var result = new List<QueueWithTopEnqueuedJobsDto>(tuples.Length);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var tuple in tuples)
            {
                var enqueuedJobIds = tuple.Monitoring.GetEnqueuedJobIds(tuple.Queue, 0, 5);
                var counters = tuple.Monitoring.GetEnqueuedAndFetchedCount(tuple.Queue);

                // TODO: Remove the Select method call to support `bigint`.
                var firstJobs = UseConnection(connection => 
                    EnqueuedJobs(connection, enqueuedJobIds.Select(x => (long)x).ToArray()));

                result.Add(new QueueWithTopEnqueuedJobsDto
                {
                    Name = tuple.Queue,
                    Length = counters.EnqueuedCount ?? 0,
                    Fetched = counters.FetchedCount,
                    FirstJobs = firstJobs
                });
            }

            return result;
        }

        public JobList<EnqueuedJobDto> EnqueuedJobs(string queue, int @from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var enqueuedJobIds = queueApi.GetEnqueuedJobIds(queue, from, perPage);

            // TODO: Remove the Select method call to support `bigint`.
            return UseConnection(connection => EnqueuedJobs(connection, enqueuedJobIds.Select(x => (long)x).ToArray()));
        }

        public JobList<FetchedJobDto> FetchedJobs(string queue, int @from, int perPage)
        {
            var queueApi = GetQueueApi(queue);
            var fetchedJobIds = queueApi.GetFetchedJobIds(queue, from, perPage);

            // TODO: Remove the Select method call to support `bigint`.
            return UseConnection(connection => FetchedJobs(connection, fetchedJobIds.Select(x => (long)x).ToArray()));
        }

        public IDictionary<DateTime, long> HourlySucceededJobs()
        {
            return UseConnection(connection => 
                GetHourlyTimelineStats(connection, "succeeded"));
        }

        public IDictionary<DateTime, long> HourlyFailedJobs()
        {
            return UseConnection(connection => 
                GetHourlyTimelineStats(connection, "failed"));
        }

        public JobDetailsDto JobDetails(string jobId)
        {
            var multi = UseConnection(connection =>
            SqlRepository.GetJobDetails(connection, _storage.SchemaName, _storage.CommandTimeout, jobId));
            var job = multi.job;
            if (job == null) return null;
            var history = multi.states
                .Select(x => new StateHistoryDto
                {
                    StateName = x.Name,
                    CreatedAt = x.CreatedAt,
                    Reason = x.Reason,
                    Data = new SafeDictionary<string, string>(
                                    JobHelper.FromJson<Dictionary<string, string>>(x.Data),
                                    StringComparer.OrdinalIgnoreCase),
                }).ToList();

            return new JobDetailsDto
            {
                CreatedAt = job.CreatedAt,
                ExpireAt = job.ExpireAt,
                Job = DeserializeJob(job.InvocationData, job.Arguments),
                History = history,
                Properties = multi.parameters
            };
        }

        public long SucceededListCount()
        {
            return UseConnection(connection => 
                GetNumberOfJobsByStateName(connection, SucceededState.StateName));
        }

        public long DeletedListCount()
        {
            return UseConnection(connection => 
                GetNumberOfJobsByStateName(connection, DeletedState.StateName));
        }

        public StatisticsDto GetStatistics()
        {
            var statistics = UseConnection(connection =>
                SqlRepository.GetStatistics(connection, _storage.SchemaName, _storage.CommandTimeout));

            statistics.Queues = _storage.QueueProviders
                .SelectMany(x => x.GetJobQueueMonitoringApi().GetQueues())
                .Count();

            return statistics;
        }

        private Dictionary<DateTime, long> GetHourlyTimelineStats(DbConnection connection, string type)
        {
            var endDate = DateTime.UtcNow;
            var dates = new List<DateTime>();
            for (var i = 0; i < 24; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddHours(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x.ToString("yyyy-MM-dd-HH")}", x => x);

            return GetTimelineStats(connection, keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(DbConnection connection, string type)
        {
            var endDate = DateTime.UtcNow.Date;
            var dates = new List<DateTime>();
            for (var i = 0; i < 7; i++)
            {
                dates.Add(endDate);
                endDate = endDate.AddDays(-1);
            }

            var keyMaps = dates.ToDictionary(x => $"stats:{type}:{x.ToString("yyyy-MM-dd")}", x => x);

            return GetTimelineStats(connection, keyMaps);
        }

        private Dictionary<DateTime, long> GetTimelineStats(
            DbConnection connection,
            IDictionary<string, DateTime> keyMaps)
        {
            var valuesMap = SqlRepository.GetTimelineStats(connection, _storage.SchemaName, _storage.CommandTimeout, keyMaps.Keys);

            foreach (var key in keyMaps.Keys)
            {
                if (!valuesMap.ContainsKey(key)) valuesMap.Add(key, 0);
            }

            var result = new Dictionary<DateTime, long>();
            for (var i = 0; i < keyMaps.Count; i++)
            {
                var value = valuesMap[keyMaps.ElementAt(i).Key];
                result.Add(keyMaps.ElementAt(i).Value, value);
            }

            return result;
        }

        private IPersistentJobQueueMonitoringApi GetQueueApi(string queueName)
        {
            var provider = _storage.QueueProviders.GetProvider(queueName);
            var monitoringApi = provider.GetJobQueueMonitoringApi();

            return monitoringApi;
        }

        private T UseConnection<T>(Func<DbConnection, T> action)
        {
            return _storage.UseConnection(null, action);
        }

        private JobList<EnqueuedJobDto> EnqueuedJobs(DbConnection connection, long[] jobIds)
        {
            var jobs = SqlRepository.GetEnqueuedJobs(connection, _storage.SchemaName, _storage.CommandTimeout, jobIds);

            var sortedSqlJobs = jobIds
                .Select(jobId => jobs.ContainsKey(jobId) ? jobs[jobId] : new SqlJob { Id = jobId })
                .ToList();
            
            return DeserializeJobs(
                sortedSqlJobs,
                (sqlJob, job, stateData) => new EnqueuedJobDto
                {
                    Job = job,
                    State = sqlJob.StateName,
                    InEnqueuedState = EnqueuedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase),
                    EnqueuedAt = EnqueuedState.StateName.Equals(sqlJob.StateName, StringComparison.OrdinalIgnoreCase)
                        ? JobHelper.DeserializeNullableDateTime(stateData["EnqueuedAt"])
                        : null
                });
        }

        private long GetNumberOfJobsByStateName(DbConnection connection, string stateName)
        {
            var count = SqlRepository.GetNumberOfJobsByStateName(connection, _storage.SchemaName, _storage.CommandTimeout, stateName, _jobListLimit);

            return count;
        }

        private static Job DeserializeJob(string invocationData, string arguments)
        {
            var data = JobHelper.FromJson<InvocationData>(invocationData);
            data.Arguments = arguments;

            try
            {
                return data.Deserialize();
            }
            catch (JobLoadException)
            {
                return null;
            }
        }

        private JobList<TDto> GetJobs<TDto>(
            DbConnection connection,
            int from,
            int count,
            string stateName,
            Func<SqlJob, Job, SafeDictionary<string, string>, TDto> selector)
        {
            var jobs = SqlRepository.GetJobs(connection, _storage.SchemaName, _storage.CommandTimeout, from, count, stateName);

            return DeserializeJobs(jobs, selector);
        }

        private static JobList<TDto> DeserializeJobs<TDto>(
            ICollection<SqlJob> jobs,
            Func<SqlJob, Job, SafeDictionary<string, string>, TDto> selector)
        {
            var result = new List<KeyValuePair<string, TDto>>(jobs.Count);
            
            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                var dto = default(TDto);
                
                if (job.InvocationData != null)
                {
                    var deserializedData = JobHelper.FromJson<Dictionary<string, string>>(job.StateData);
                    var stateData = deserializedData != null
                        ? new SafeDictionary<string, string>(deserializedData, StringComparer.OrdinalIgnoreCase)
                        : null;

                    dto = selector(job, DeserializeJob(job.InvocationData, job.Arguments), stateData);
                }

                result.Add(new KeyValuePair<string, TDto>(
                    job.Id.ToString(), dto));
            }

            return new JobList<TDto>(result);
        }

        private JobList<FetchedJobDto> FetchedJobs(DbConnection connection, IEnumerable<long> jobIds)
        {
            var jobs = SqlRepository.GetFetchedJobs(connection, _storage.SchemaName, _storage.CommandTimeout, jobIds);

            var result = new List<KeyValuePair<string, FetchedJobDto>>(jobs.Count);

            // ReSharper disable once LoopCanBeConvertedToQuery
            foreach (var job in jobs)
            {
                result.Add(new KeyValuePair<string, FetchedJobDto>(
                    job.Id.ToString(),
                    new FetchedJobDto
                    {
                        Job = DeserializeJob(job.InvocationData, job.Arguments),
                        State = job.StateName,
                    }));
            }

            return new JobList<FetchedJobDto>(result);
        }

        /// <summary>
        /// Overloaded dictionary that doesn't throw if given an invalid key
        /// Fixes issues such as https://github.com/HangfireIO/Hangfire/issues/871
        /// </summary>
        private class SafeDictionary<TKey, TValue> : Dictionary<TKey, TValue>
        {
            public SafeDictionary(IDictionary<TKey, TValue> dictionary, IEqualityComparer<TKey> comparer)
                : base(dictionary, comparer)
            {
            }

            public new TValue this[TKey i]
            {
                get { return ContainsKey(i) ? base[i] : default(TValue); }
                set { base[i] = value; }
            }
        }
    }
}

