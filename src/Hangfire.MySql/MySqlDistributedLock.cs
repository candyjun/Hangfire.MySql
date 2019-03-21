using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Dapper;
using Hangfire.Annotations;
using Hangfire.Storage;

namespace Hangfire.MySql
{
    public class MySqlDistributedLock
    {
        private static readonly TimeSpan LockTimeout = TimeSpan.FromSeconds(5);

        private const string LockMode = "Exclusive";
        private const string LockOwner = "Session";

        private static readonly IDictionary<int, string> LockErrorMessages
            = new Dictionary<int, string>
            {
                { -1, "The lock request timed out" },
                { -2, "The lock request was canceled" },
                { -3, "The lock request was chosen as a deadlock victim" },
                { -999, "Indicates a parameter validation or other call error" }
            };

        internal static void Acquire(IDbConnection connection, string resource, TimeSpan timeout)
        {
            if (connection.State != ConnectionState.Open)
            {
                // When we are passing a closed connection to Dapper's Execute method,
                // it kindly opens it for us, but after command execution, it will be closed
                // automatically, and our just-acquired application lock will immediately
                // be released. This is not behavior we want to achieve, so let's throw an
                // exception instead.
                throw new InvalidOperationException("Connection must be open before acquiring a distributed lock.");
            }

            var started = Stopwatch.StartNew();

            // We can't pass our timeout directly to the GET_LOCK stored procedure, because
            // high values, such as minute or more, may cause SQL Server's thread pool starvation,
            // when the number of connections that try to acquire a lock is more than the number of 
            // available threads in SQL Server. In this case a deadlock will occur, when SQL Server 
            // tries to schedule some more work for a connection that acquired a lock, but all the 
            // available threads in a pool waiting for that lock to be released.
            //
            // So we are trying to acquire a lock multiple times instead, with timeout that's equal
            // to seconds, not minutes.
            var lockTimeout = (long) Math.Min(LockTimeout.TotalMilliseconds, timeout.TotalMilliseconds);

            do
            {
                var parameters = new DynamicParameters();
                parameters.Add("@Resource", resource);
                parameters.Add("@DbPrincipal", "public");
                parameters.Add("@LockMode", LockMode);
                parameters.Add("@LockOwner", LockOwner);
                parameters.Add("@LockTimeout", lockTimeout);
                parameters.Add("@Result", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

                connection.Execute(
                    @"GET_LOCK",
                    parameters,
                    commandTimeout: (int) (lockTimeout / 1000) + 5,
                    commandType: CommandType.StoredProcedure);

                var lockResult = parameters.Get<int>("@Result");

                if (lockResult >= 0)
                {
                    // The lock has been successfully obtained on the specified resource.
                    return;
                }

                if (lockResult == -999 /* Indicates a parameter validation or other call error. */)
                {
                    throw new MySqlDistributedLockException(
                        $"Could not place a lock on the resource '{resource}': {(LockErrorMessages.ContainsKey(lockResult) ? LockErrorMessages[lockResult] : $"Server returned the '{lockResult}' error.")}.");
                }
            } while (started.Elapsed < timeout);

            throw new DistributedLockTimeoutException(resource);
        }

        internal static void Release(IDbConnection connection, string resource)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@Resource", resource);
            parameters.Add("@LockOwner", LockOwner);
            parameters.Add("@Result", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

            connection.Execute(
                @"RELEASE_LOCK",
                parameters,
                commandType: CommandType.StoredProcedure);

            var releaseResult = parameters.Get<int>("@Result");

            if (releaseResult < 0)
            {
                throw new MySqlDistributedLockException(
                    $"Could not release a lock on the resource '{resource}': Server returned the '{releaseResult}' error.");
            }
        }
    }
}
