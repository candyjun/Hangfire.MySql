using MySql.Data.MySqlClient;
using System;
using System.Linq.Expressions;
using System.Reflection;

namespace Hangfire.MySql
{
    internal class SqlCommandSet : IDisposable
    {
        public  static readonly bool IsAvailable;

        private static readonly Type SqlCommandSetType;
        private readonly object _instance;

        private static readonly Action<object, MySqlConnection> SetConnection;
        private static readonly Action<object, MySqlTransaction> SetTransaction;
        private static readonly Func<object, MySqlCommand> GetBatchCommand;
        private static readonly Action<object, MySqlCommand> AppendMethod;
        private static readonly Func<object, int> ExecuteNonQueryMethod;
        private static readonly Action<object> DisposeMethod;

        static SqlCommandSet()
        {
            try
            {
                var typeAssembly = typeof(MySqlCommand).GetTypeInfo().Assembly;
                SqlCommandSetType = typeAssembly.GetType("System.Data.SqlClient.SqlCommandSet");

                if (SqlCommandSetType == null) return;

                var p = Expression.Parameter(typeof(object));
                var converted = Expression.Convert(p, SqlCommandSetType);

                var connectionParameter = Expression.Parameter(typeof(SqlConnection));
                var transactionParameter = Expression.Parameter(typeof(MySqlTransaction));
                var commandParameter = Expression.Parameter(typeof(MySqlCommand));

                SetConnection = Expression.Lambda<Action<object, MySqlConnection>>(Expression.Call(converted, "set_Connection", null, connectionParameter), p, connectionParameter).Compile();
                SetTransaction = Expression.Lambda<Action<object, MySqlTransaction>>(Expression.Call(converted, "set_Transaction", null, transactionParameter), p, transactionParameter).Compile();
                GetBatchCommand = Expression.Lambda<Func<object, MySqlCommand>>(Expression.Call(converted, "get_BatchCommand", null), p).Compile();
                AppendMethod = Expression.Lambda<Action<object, MySqlCommand>>(Expression.Call(converted, "Append", null, commandParameter), p, commandParameter).Compile();
                ExecuteNonQueryMethod = Expression.Lambda<Func<object, int>>(Expression.Call(converted, "ExecuteNonQuery", null), p).Compile();
                DisposeMethod = Expression.Lambda<Action<object>>(Expression.Call(converted, "Dispose", null), p).Compile();

                IsAvailable = true;
            }
            catch (Exception)
            {
                IsAvailable = false;
            }
        }

        public SqlCommandSet()
        {
            if (!IsAvailable)
            {
                throw new PlatformNotSupportedException("SqlCommandSet is not supported on this platform, use regular commands instead");
            }

            _instance = Activator.CreateInstance(SqlCommandSetType, true);
        }

        public MySqlConnection Connection
        {
            set { SetConnection(_instance, value); }
        }

        public MySqlTransaction Transaction
        {
            set { SetTransaction(_instance, value); }
        }

        public MySqlCommand BatchCommand => GetBatchCommand(_instance);
        public int CommandCount { get; private set; }

        public void Append(MySqlCommand command)
        {
            AppendMethod(_instance, command);
            CommandCount++;
        }

        public int ExecuteNonQuery()
        {
            if (CommandCount == 0)
            {
                return 0;
            }

            return ExecuteNonQueryMethod(_instance);
        }

        public void Dispose()
        {
            DisposeMethod(_instance);
        }
    }
}