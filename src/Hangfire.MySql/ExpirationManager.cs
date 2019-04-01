using System;
using System.Data.Common;
using System.Threading;
using Hangfire.Common;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;

namespace Hangfire.MySql
{
#pragma warning disable 618
    internal class ExpirationManager : IServerComponent
#pragma warning restore 618
    {
        private const string DistributedLockKey = "locks:expirationmanager";
        private static readonly TimeSpan DefaultLockTimeout = TimeSpan.FromMinutes(5);
        
        // This value should be high enough to optimize the deletion as much, as possible,
        // reducing the number of queries. But low enough to cause lock escalations (it
        // appears, when ~5000 locks were taken, but this number is a subject of version).
        // Note, that lock escalation may also happen during the cascade deletions for
        // State (3-5 rows/job usually) and JobParameters (2-3 rows/job usually) tables.
        private const int NumberOfRecordsInSinglePass = 1000;
        
        private static readonly string[] ProcessedTables =
        {
            "AggregatedCounter",
            "Job",
            "List",
            "Set",
            "Hash",
        };

        private readonly ILog _logger = LogProvider.For<ExpirationManager>();
        private readonly MySqlStorage _storage;
        private readonly TimeSpan _checkInterval;

        public ExpirationManager(MySqlStorage storage, TimeSpan checkInterval)
        {
            _storage = storage ?? throw new ArgumentNullException(nameof(storage));
            _checkInterval = checkInterval;
        }

        public void Execute(CancellationToken cancellationToken)
        {
            foreach (var table in ProcessedTables)
            {
                _logger.Debug($"Removing outdated records from the '{table}' table...");

                UseConnectionDistributedLock(_storage, connection =>
                {
                    int affected;

                    do
                    {
                        affected = SqlRepository.RemovingOutdatedRecords(connection, _storage.SchemaName, table, NumberOfRecordsInSinglePass, DateTime.UtcNow, cancellationToken);

                    } while (affected == NumberOfRecordsInSinglePass);
                });

                _logger.Trace($"Outdated records removed from the '{table}' table.");
            }

            cancellationToken.Wait(_checkInterval);
        }

        public override string ToString()
        {
            return GetType().ToString();
        }

        private void UseConnectionDistributedLock(MySqlStorage storage, Action<DbConnection> action)
        {
            try
            {
                storage.UseConnection(null, connection =>
                {
                    MySqlDistributedLock.Acquire(connection, DistributedLockKey, DefaultLockTimeout);

                    try
                    {
                        action(connection);
                    }
                    finally
                    {
                        MySqlDistributedLock.Release(connection, DistributedLockKey);
                    }
                });
            }
            catch (DistributedLockTimeoutException e) when (e.Resource == DistributedLockKey)
            {
                // DistributedLockTimeoutException here doesn't mean that outdated records weren't removed.
                // It just means another Hangfire server did this work.
                _logger.Log(
                    LogLevel.Debug,
                    () => $@"An exception was thrown during acquiring distributed lock on the {DistributedLockKey} resource within {DefaultLockTimeout.TotalSeconds} seconds. Outdated records were not removed.
It will be retried in {_checkInterval.TotalSeconds} seconds.",
                    e);
            }
        }
    }
}

