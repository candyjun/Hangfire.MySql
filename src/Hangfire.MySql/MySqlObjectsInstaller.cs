using System;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Reflection;
using Dapper;
using Hangfire.Logging;

namespace Hangfire.MySql
{
    public static class MySqlObjectsInstaller
    {
        public static readonly int RequiredSchemaVersion = 5;
        private const int RetryAttempts = 3;

        public static void Install(DbConnection connection)
        {
            Install(connection, null);
        }

        public static void Install(DbConnection connection, string schema)
        {
            if (connection == null) throw new ArgumentNullException(nameof(connection));

            var log = LogProvider.GetLogger(typeof(MySqlObjectsInstaller));

            log.Info("Start installing Hangfire SQL objects...");

            var script = GetStringResource(
                typeof(MySqlObjectsInstaller).GetTypeInfo().Assembly, 
                "Hangfire.MySql.Install.sql");

            script = script.Replace("SET @TARGET_SCHEMA_VERSION = 5;", "SET @TARGET_SCHEMA_VERSION = " + RequiredSchemaVersion + ";");

            script = script.Replace("$(HangFireSchema)", !string.IsNullOrWhiteSpace(schema) ? schema : Constants.DefaultSchema);

            connection.Execute(script, commandTimeout: 0);

            log.Info("Hangfire SQL objects installed.");
        }

        private static string GetStringResource(Assembly assembly, string resourceName)
        {
            using (var stream = assembly.GetManifestResourceStream(resourceName))
            {
                if (stream == null) 
                {
                    throw new InvalidOperationException(
                        $"Requested resource `{resourceName}` was not found in the assembly `{assembly}`.");
                }

                using (var reader = new StreamReader(stream))
                {
                    return reader.ReadToEnd();
                }
            }
        }
    }
}
