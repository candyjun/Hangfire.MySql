using Hangfire.Annotations;
using Hangfire.Dashboard;
using Hangfire.Logging;
using Hangfire.Server;
using Hangfire.Storage;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Text;

namespace Hangfire.MySql
{
    public class MySqlStorage : JobStorage
    {
        private readonly DbConnection _existingConnection;
        private readonly Func<DbConnection> _connectionFactory;
        private readonly MySqlStorageOptions _options;
        private readonly string _connectionString;

        public MySqlStorage(string nameOrConnectionString)
            : this(nameOrConnectionString, new MySqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes MySqlStorage from the provided MySqlStorageOptions and either the provided connection
        /// string or the connection string with provided name pulled from the application config file.       
        /// </summary>
        /// <param name="nameOrConnectionString">Either a SQL Server connection string or the name of 
        /// a SQL Server connection string located in the connectionStrings node in the application config</param>
        /// <param name="options"></param>
        /// <exception cref="ArgumentNullException"><paramref name="nameOrConnectionString"/> argument is null.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="options"/> argument is null.</exception>
        /// <exception cref="ArgumentException"><paramref name="nameOrConnectionString"/> argument is neither 
        /// a valid SQL Server connection string nor the name of a connection string in the application
        /// config file.</exception>
        public MySqlStorage(string nameOrConnectionString, MySqlStorageOptions options)
        {
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            _connectionString = GetConnectionString(nameOrConnectionString);
            _connectionFactory = () => new MySqlConnection(_connectionString);
            _options = options ?? throw new ArgumentNullException(nameof(options));

            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlStorage"/> class with
        /// explicit instance of the <see cref="DbConnection"/> class that will be used
        /// to query the data.
        /// </summary>
        public MySqlStorage([NotNull] DbConnection existingConnection)
            : this(existingConnection, new MySqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlStorage"/> class with
        /// explicit instance of the <see cref="DbConnection"/> class that will be used
        /// to query the data, with the given options.
        /// </summary>
        public MySqlStorage([NotNull] DbConnection existingConnection, [NotNull] MySqlStorageOptions options)
        {
            _existingConnection = existingConnection ?? throw new ArgumentNullException(nameof(existingConnection));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            Initialize();
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlStorage"/> class with
        /// a connection factory <see cref="Func{DbConnection}"/> class that will be invoked
        /// to create new database connections for querying the data.
        /// </summary>
        public MySqlStorage([NotNull] Func<DbConnection> connectionFactory)
            : this(connectionFactory, new MySqlStorageOptions())
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="MySqlStorage"/> class with
        /// a connection factory <see cref="Func{DbConnection}"/> class that will be invoked
        /// to create new database connections for querying the data.
        /// </summary>
        public MySqlStorage([NotNull] Func<DbConnection> connectionFactory, [NotNull] MySqlStorageOptions options)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _options = options ?? throw new ArgumentNullException(nameof(options));

            Initialize();
        }

        public virtual PersistentJobQueueProviderCollection QueueProviders { get; private set; }

        internal string SchemaName => _options.SchemaName;
        internal int? CommandTimeout => _options.CommandTimeout.HasValue ? (int)_options.CommandTimeout.Value.TotalSeconds : (int?)null;
        internal int? CommandBatchMaxTimeout => _options.CommandBatchMaxTimeout.HasValue ? (int)_options.CommandBatchMaxTimeout.Value.TotalSeconds : (int?)null;
        internal TimeSpan? SlidingInvisibilityTimeout => _options.SlidingInvisibilityTimeout;

        public override IMonitoringApi GetMonitoringApi()
        {
            return new MySqlMonitoringApi(this, _options.DashboardJobListLimit);
        }

        public override IStorageConnection GetConnection()
        {
            return new SqlConnection(this);
        }

#pragma warning disable 618
        public override IEnumerable<IServerComponent> GetComponents()
#pragma warning restore 618
        {
            yield return new ExpirationManager(this, _options.JobExpirationCheckInterval);
            yield return new CountersAggregator(this, _options.CountersAggregateInterval);
        }

        public override void WriteOptionsToLog(ILog logger)
        {
            logger.Info("Using the following options for SQL Server job storage:");
            logger.Info($"    Queue poll interval: {_options.QueuePollInterval}.");
        }

        public override string ToString()
        {
            const string canNotParseMessage = "<Connection string can not be parsed>";

            try
            {
                var parts = _connectionString.Split(new[] { ';' }, StringSplitOptions.RemoveEmptyEntries)
                    .Select(x => x.Split(new[] { '=' }, StringSplitOptions.RemoveEmptyEntries))
                    .Select(x => new { Key = x[0].Trim(), Value = x[1].Trim() })
                    .ToDictionary(x => x.Key, x => x.Value, StringComparer.OrdinalIgnoreCase);

                var builder = new StringBuilder();

                foreach (var alias in new[] { "Data Source", "Server", "Address", "Addr", "Network Address" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                if (builder.Length != 0) builder.Append("@");

                foreach (var alias in new[] { "Database", "Initial Catalog" })
                {
                    if (parts.ContainsKey(alias))
                    {
                        builder.Append(parts[alias]);
                        break;
                    }
                }

                return builder.Length != 0
                    ? $"SQL Server: {builder}"
                    : canNotParseMessage;
            }
            catch (Exception)
            {
                return canNotParseMessage;
            }
        }

        internal void UseConnection(DbConnection dedicatedConnection, [InstantHandle] Action<DbConnection> action)
        {
            UseConnection(dedicatedConnection, connection =>
            {
                action(connection);
                return true;
            });
        }

        internal T UseConnection<T>(DbConnection dedicatedConnection, [InstantHandle] Func<DbConnection, T> func)
        {
            DbConnection connection = null;

            try
            {
                connection = dedicatedConnection ?? CreateAndOpenConnection();
                return func(connection);
            }
            finally
            {
                if (dedicatedConnection == null)
                {
                    ReleaseConnection(connection);
                }
            }
        }

        internal void UseTransaction(DbConnection dedicatedConnection, [InstantHandle] Action<DbConnection, DbTransaction> action)
        {
            UseTransaction(dedicatedConnection, (connection, transaction) =>
            {
                action(connection, transaction);
                return true;
            }, null);
        }
        
        internal T UseTransaction<T>(
            DbConnection dedicatedConnection,
            [InstantHandle] Func<DbConnection, DbTransaction, T> func, 
            IsolationLevel? isolationLevel)
        {
            return UseConnection(dedicatedConnection, connection =>
            {
                using (var transaction = connection.BeginTransaction(isolationLevel ?? IsolationLevel.ReadCommitted))
                {
                    var result = func(connection, transaction);
                    transaction.Commit();

                    return result;
                }
            });
        }

        internal DbConnection CreateAndOpenConnection()
        {
            var connection = _existingConnection
                ?? _connectionFactory();

            if (connection.State == ConnectionState.Closed)
            {
                connection.Open();
            }

            return connection;
        }

        internal bool IsExistingConnection(IDbConnection connection)
        {
            return connection != null && ReferenceEquals(connection, _existingConnection);
        }

        internal void ReleaseConnection(IDbConnection connection)
        {
            if (connection != null && !IsExistingConnection(connection))
            {
                connection.Dispose();
            }
        }

        private void Initialize()
        {
            if (_options.PrepareSchemaIfNecessary)
            {
                UseConnection(null, connection =>
                {
                    MySqlObjectsInstaller.Install(connection, _options.SchemaName);
                });
            }

            InitializeQueueProviders();
        }

        private void InitializeQueueProviders()
        {
            var defaultQueueProvider = new MySqlJobQueueProvider(this, _options);
            QueueProviders = new PersistentJobQueueProviderCollection(defaultQueueProvider);
        }

        private string GetConnectionString(string nameOrConnectionString)
        {
            return nameOrConnectionString;
        }

        public static readonly DashboardMetric ActiveConnections = new DashboardMetric(
            "connections:active",
            "Metrics_ActiveConnections",
            page =>
            {
                if (!(page.Storage is MySqlStorage sqlStorage)) return new Metric("???");

                return sqlStorage.UseConnection(null, connection =>
                {
                    var value = SqlRepository.GetActiveConnections(connection);

                    return new Metric(value);
                });
            });

        public static readonly DashboardMetric TotalConnections = new DashboardMetric(
            "connections:total",
            "Metrics_TotalConnections",
            page =>
            {
                if (!(page.Storage is MySqlStorage sqlStorage)) return new Metric("???");

                return sqlStorage.UseConnection(null, connection =>
                {
                    var value = SqlRepository.GetTotalConnections(connection);

                    return new Metric(value);
                });
            });
    }
}