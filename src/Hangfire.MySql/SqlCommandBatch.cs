using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data.Common;

namespace Hangfire.MySql
{
    internal class SqlCommandBatch : IDisposable
    {
        private readonly List<DbCommand> _commandList = new List<DbCommand>();
        private readonly SqlCommandSet _commandSet;
        private readonly int _defaultTimeout;

        public SqlCommandBatch(bool preferBatching)
        {
            if (SqlCommandSet.IsAvailable && preferBatching)
            {
                try
                {
                    _commandSet = new SqlCommandSet();
                    _defaultTimeout = _commandSet.BatchCommand.CommandTimeout;
                }
                catch (Exception)
                {
                    _commandSet = null;
                }
            }
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

            _commandSet?.Dispose();
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
            if (_commandSet != null && command is MySqlCommand)
            {
                _commandSet.Append((MySqlCommand)command);
            }
            else
            {
                _commandList.Add(command);
            }
        }

        public void ExecuteNonQuery()
        {
            if (_commandSet != null && _commandSet.CommandCount > 0)
            {
                _commandSet.Connection = Connection as MySqlConnection;
                _commandSet.Transaction = Transaction as MySqlTransaction;

                var batchTimeout = CommandTimeout ?? _defaultTimeout;

                if (batchTimeout > 0)
                {
                    batchTimeout = batchTimeout * _commandSet.CommandCount;

                    if (CommandBatchMaxTimeout.HasValue)
                    {
                        batchTimeout = Math.Min(CommandBatchMaxTimeout.Value, batchTimeout);
                    }
                }

                _commandSet.BatchCommand.CommandTimeout = batchTimeout;
                _commandSet.ExecuteNonQuery();
            }

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