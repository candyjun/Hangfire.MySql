using System;
using System.Data.Common;
using Hangfire.Annotations;
using Hangfire.MySql;

namespace Hangfire
{
    public static class MySqlStorageExtensions
    {
        public static IGlobalConfiguration<MySqlStorage> UseMySqlStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));

            var storage = new MySqlStorage(nameOrConnectionString);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<MySqlStorage> UseMySqlStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] string nameOrConnectionString, 
            [NotNull] MySqlStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (nameOrConnectionString == null) throw new ArgumentNullException(nameof(nameOrConnectionString));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var storage = new MySqlStorage(nameOrConnectionString, options);
            return configuration.UseStorage(storage);
        }

        public static IGlobalConfiguration<MySqlStorage> UseMySqlStorage(
            [NotNull] this IGlobalConfiguration configuration,
            [NotNull] Func<DbConnection> connectionFactory,
            [NotNull] MySqlStorageOptions options)
        {
            if (configuration == null) throw new ArgumentNullException(nameof(configuration));
            if (connectionFactory == null) throw new ArgumentNullException(nameof(connectionFactory));
            if (options == null) throw new ArgumentNullException(nameof(options));

            var storage = new MySqlStorage(connectionFactory, options);
            return configuration.UseStorage(storage);
        }
    }
}
