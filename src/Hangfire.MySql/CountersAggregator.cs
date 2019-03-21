using System;
using System.Threading;
using Dapper;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;

namespace Hangfire.MySql
{
#pragma warning disable 618
    internal class CountersAggregator : IServerComponent
#pragma warning restore 618
    {
        // This number should be high enough to aggregate counters efficiently,
        // but low enough to not to cause large amount of row locks to be taken.
        // Lock escalation to page locks may pause the background processing.
        private const int NumberOfRecordsInSinglePass = 1000;
        private static readonly TimeSpan DelayBetweenPasses = TimeSpan.FromMilliseconds(500);

        private readonly ILog _logger = LogProvider.For<CountersAggregator>();
        private readonly MySqlStorage _storage;
        private readonly TimeSpan _interval;

        public CountersAggregator(MySqlStorage storage, TimeSpan interval)
        {
            if (storage == null) throw new ArgumentNullException(nameof(storage));

            _storage = storage;
            _interval = interval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            _logger.Debug("Aggregating records in 'Counter' table...");

            int removedCount = 0;

            do
            {
                _storage.UseConnection(null, connection =>
                {
                    removedCount = connection.Execute(
                        GetAggregationQuery(_storage),
                        new { now = DateTime.UtcNow, count = NumberOfRecordsInSinglePass },
                        commandTimeout: 0);
                });

                if (removedCount >= NumberOfRecordsInSinglePass)
                {
                    cancellationToken.Wait(DelayBetweenPasses);
                    cancellationToken.ThrowIfCancellationRequested();
                }
                // ReSharper disable once LoopVariableIsNeverChangedInsideLoop
            } while (removedCount >= NumberOfRecordsInSinglePass);

            _logger.Trace("Records from the 'Counter' table aggregated.");

            cancellationToken.Wait(_interval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private static string GetAggregationQuery(MySqlStorage storage)
        {
            return @"
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

INSERT INTO  " + storage.SchemaName + @"_AggregatedCounter (`Key`, Value, ExpireAt)
    SELECT `Key`, SUM(Value) as Value, MAX(ExpireAt) AS ExpireAt 
    FROM (
            SELECT `Key`, Value, ExpireAt
            FROM  " + storage.SchemaName + @"_Counter
            LIMIT @count) tmp
	GROUP BY `Key`
        ON DUPLICATE KEY UPDATE 
            Value = Value + VALUES(Value),
            ExpireAt = GREATEST(ExpireAt,VALUES(ExpireAt));

DELETE FROM `" + storage.SchemaName + @"_Counter`
LIMIT @count;

COMMIT;";
        }
    }
}

