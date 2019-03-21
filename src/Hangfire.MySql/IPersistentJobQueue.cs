using System.Threading;
using Hangfire.Storage;

namespace Hangfire.MySql
{
    public interface IPersistentJobQueue
    {
        IFetchedJob Dequeue(string[] queues, CancellationToken cancellationToken);

        void Enqueue(
            System.Data.Common.DbConnection connection, 
            System.Data.Common.DbTransaction transaction, 
            string queue, 
            string jobId);

    }
}