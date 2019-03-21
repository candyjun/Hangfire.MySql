using System.Collections.Generic;

namespace Hangfire.MySql
{
    public interface IPersistentJobQueueMonitoringApi
    {
        IEnumerable<string> GetQueues();

        // TODO: Change return type to `IEnumerable<long>` to support `bigint` type.
        IEnumerable<int> GetEnqueuedJobIds(string queue, int from, int perPage);

        // TODO: Change return type to `IEnumerable<long>` to support `bigint` type.
        IEnumerable<int> GetFetchedJobIds(string queue, int from, int perPage);

        EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(string queue);
    }
}
