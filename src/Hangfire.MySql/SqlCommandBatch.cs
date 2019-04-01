using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Hangfire.MySql
{
    internal class SqlCommandBatch : IDisposable
    {
        private readonly List<DbCommand> _commandList = new List<DbCommand>();

        public SqlCommandBatch()
        {

        }

        public DbConnection Connection { get; set; }
        public DbTransaction Transaction { get; set; }

        public int? CommandTimeout { get; set; }
        public int? CommandBatchMaxTimeout { get; set; }

        public void Dispose()
        {
            foreach (var command in _commandList)
            {
                command.Dispose();
            }
        }

        public void Append(string commandText, params MySqlParameter[] parameters)
        {
            var command = new MySqlCommand(commandText);

            foreach (var parameter in parameters)
            {
                command.Parameters.Add(parameter);
            }

            Append(command);
        }

        public void Append(DbCommand command)
        {
            _commandList.Add(command);
        }

        public void ExecuteNonQuery()
        {
            foreach (var command in _commandList)
            {
                command.Connection = Connection;
                command.Transaction = Transaction;

                if (CommandTimeout.HasValue)
                {
                    command.CommandTimeout = CommandTimeout.Value;
                }

                command.ExecuteNonQuery();
            }
        }
    }
}