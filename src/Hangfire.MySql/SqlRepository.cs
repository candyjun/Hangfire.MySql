using Dapper;
using Hangfire.Common;
using Hangfire.MySql.Entities;
using Hangfire.Storage;
using Hangfire.Storage.Monitoring;
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading;

namespace Hangfire.MySql
{
    /// <summary>
    /// sql repository
    /// </summary>
    public class SqlRepository
    {
        public static int GetAggregation(IDbConnection connection, string schemaName, int count)
        {
            var sql = @"
SET TRANSACTION ISOLATION LEVEL READ COMMITTED;
START TRANSACTION;

INSERT INTO  " + schemaName + @"_AggregatedCounter (`Key`, Value, ExpireAt)
    SELECT `Key`, SUM(Value) as Value, MAX(ExpireAt) AS ExpireAt 
    FROM (
            SELECT `Key`, Value, ExpireAt
            FROM  " + schemaName + @"_Counter
            LIMIT @count) tmp
	GROUP BY `Key`
        ON DUPLICATE KEY UPDATE 
            Value = Value + VALUES(Value),
            ExpireAt = GREATEST(ExpireAt,VALUES(ExpireAt));

DELETE FROM `" + schemaName + @"_Counter`
LIMIT @count;

COMMIT;";
            return connection.Execute(sql, new { now = DateTime.UtcNow, count }, commandTimeout: 0);
        }

        public static int RemovingOutdatedRecords(IDbConnection connection, string schemaName, string table,
            int count, DateTime expireAt,
            CancellationToken cancellationToken)
        {
            var sql = $@"set transaction isolation level read committed;
delete top (@count) from `{schemaName}`.`{table}` 
where ExpireAt < @expireAt
option (loop join, optimize for (@count = 20000));";

            using (var command = connection.CreateCommand())
            {
                command.CommandText = sql;
                command.Parameters.Add(new MySqlParameter("@count", count));
                command.Parameters.Add(new MySqlParameter("@expireAt", expireAt));
                command.CommandTimeout = 0;

                using (cancellationToken.Register(state => ((MySqlCommand)state).Cancel(), command))
                {
                    try
                    {
                        return command.ExecuteNonQuery();
                    }
                    catch (MySqlException) when (cancellationToken.IsCancellationRequested)
                    {
                        // Exception was triggered due to the Cancel method call, ignoring
                        return 0;
                    }
                }
            }
        }

        public static int AcquireLock(IDbConnection connection, string resource, long lockTimeout,
            string lockMode, string lockOwner)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@Resource", resource);
            parameters.Add("@DbPrincipal", "public");
            parameters.Add("@LockMode", lockMode);
            parameters.Add("@LockOwner", lockOwner);
            parameters.Add("@LockTimeout", lockTimeout);
            parameters.Add("@Result", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

            connection.Execute(
                @"GET_LOCK",
                parameters,
                commandTimeout: (int)(lockTimeout / 1000) + 5,
                commandType: CommandType.StoredProcedure);

            return parameters.Get<int>("@Result");
        }

        public static int ReleaseLock(IDbConnection connection, string resource, string lockOwner)
        {
            var parameters = new DynamicParameters();
            parameters.Add("@Resource", resource);
            parameters.Add("@LockOwner", lockOwner);
            parameters.Add("@Result", dbType: DbType.Int32, direction: ParameterDirection.ReturnValue);

            connection.Execute(
                @"RELEASE_LOCK",
                parameters,
                commandType: CommandType.StoredProcedure);

            return parameters.Get<int>("@Result");
        }

        public static void AddJobQueue(IDbConnection connection, string schemaName,
            int? commandTimeout,
            IDbTransaction transaction, string queue, string jobId)
        {
            string enqueueJobSql =
$@"insert into `{schemaName}`.JobQueue (JobId, Queue) values (@jobId, @queue)";

            connection.Execute(
                enqueueJobSql,
                new { jobId = long.Parse(jobId), queue }
                , transaction
                , commandTimeout: commandTimeout);
        }

        public static T GetFetchedJob<T>(IDbConnection connection, string schemaName,
            string[] queues, double timeout)
        {
            var sql = $@"
set transaction isolation level read committed
update top (1) JQ
set FetchedAt = GETUTCDATE()
output INSERTED.Id, INSERTED.JobId, INSERTED.Queue
from `{schemaName}`.JobQueue JQ
where Queue in @queues and
(FetchedAt is null or FetchedAt < DATEADD(second, @timeout, GETUTCDATE()))";

            return connection.Query<T>(sql, new { queues, timeout }).SingleOrDefault();
        }

        public static T GetFetchedJobUsingTransaction<T>(IDbConnection connection, string schemaName,
            string[] queues, double timeout, IDbTransaction transaction, int? commandTimeout)
        {
            var sql = $@"delete top (1) JQ
output DELETED.Id, DELETED.JobId, DELETED.Queue
from `{schemaName}`.JobQueue JQ
where Queue in @queues and (FetchedAt is null or FetchedAt < DATEADD(second, @timeout, GETUTCDATE()))";

            return connection.Query<T>(sql, new { queues, timeout }, transaction, commandTimeout: commandTimeout)
                .SingleOrDefault();
        }

        public static List<string> GetQueues(IDbConnection connection, string schemaName, int? commandTimeout)
        {
            string sqlQuery = $@"select distinct(Queue) from `{schemaName}`.JobQueue ";

            return connection.Query(sqlQuery, commandTimeout: commandTimeout).Select(x => (string)x.Queue).ToList();
        }

        public static List<int> GetEnqueuedJobIds(IDbConnection connection, string schemaName, 
            string queue, int @from, int perPage, int? commandTimeout)
        {
            var sqlQuery =
$@"select r.JobId from (
  select jq.JobId, row_number() over (order by jq.Id) as row_num 
  from `{schemaName}`.JobQueue jq
  where jq.Queue = @queue and jq.FetchedAt is null
) as r
where r.row_num between @start and @end";

            return connection.Query<dynamic>(
                    sqlQuery,
                    new { queue, start = from + 1, end = @from + perPage },
                    commandTimeout: commandTimeout)
                    .ToList()
                    .Select(x => (int)x.JobId)
                    .ToList();
        }

        public static List<int> GetFetchedJobIds(IDbConnection connection, string schemaName, 
            string queue, int @from, int perPage)
        {
            var fetchedJobsSql = $@"
select r.JobId from (
  select jq.JobId, jq.FetchedAt, row_number() over (order by jq.Id) as row_num 
  from `{schemaName}`.JobQueue jq 
  where jq.Queue = @queue and jq.FetchedAt is not null
) as r
where r.row_num between @start and @end";

            return connection.Query<dynamic>(
                        fetchedJobsSql,
                        new { queue, start = from + 1, end = @from + perPage })
                    .ToList()
                    .Select(x => (int)x.JobId)
                    .ToList();
        }

        public static EnqueuedAndFetchedCountDto GetEnqueuedAndFetchedCount(IDbConnection connection, string schemaName, string queue)
        {
            var sqlQuery = $@"
select sum(Enqueued) as EnqueuedCount, sum(Fetched) as FetchedCount 
from (
    select 
        case when FetchedAt is null then 1 else 0 end as Enqueued,
        case when FetchedAt is not null then 1 else 0 end as Fetched
    from `{schemaName}`.JobQueue
    where Queue = @queue
) q";

            var result = connection.Query(sqlQuery, new { queue }).Single();

            return new EnqueuedAndFetchedCountDto
            {
                EnqueuedCount = result.EnqueuedCount,
                FetchedCount = result.FetchedCount
            };
        }

        public static List<Entities.Server> GetServers(IDbConnection connection, string schemaName, int? commandTimeout)
        {
            return connection.Query<Entities.Server>(
                    $@"select * from `{schemaName}`.Server", commandTimeout: commandTimeout)
                    .ToList();
        }
        
        public static (SqlJob job, List<SqlState> states, Dictionary<string, string> parameters) GetJobDetails(IDbConnection connection, string schemaName, int? commandTimeout, string jobId)
        {
            string sql = $@"
select * from `{schemaName}`.Job where Id = @id;
select * from `{schemaName}`.JobParameter where JobId = @id;
select * from `{schemaName}`.State where JobId = @id order by Id desc;";

            using (var multi = connection.QueryMultiple(sql, new { id = jobId }, commandTimeout: commandTimeout))
            {
                var job = multi.Read<SqlJob>().SingleOrDefault();
                if (job == null) return (null, null, null);

                var parameters = multi.Read<JobParameter>().ToDictionary(x => x.Name, x => x.Value);
                var states = multi.Read<SqlState>().ToList();

                return (job, states, parameters);
            }
        }

        public static StatisticsDto GetStatistics(IDbConnection connection, string schemaName, int? commandTimeout)
        {
            string sql = string.Format(@"
set transaction isolation level read committed;
select count(Id) from `{0}`.Job where StateName = N'Enqueued';
select count(Id) from `{0}`.Job where StateName = N'Failed';
select count(Id) from `{0}`.Job where StateName = N'Processing';
select count(Id) from `{0}`.Job where StateName = N'Scheduled';
select count(Id) from `{0}`.Server;
select sum(s.`Value`) from (
    select sum(`Value`) as `Value` from `{0}`.Counter where `Key` = N'stats:succeeded'
    union all
    select `Value` from `{0}`.AggregatedCounter where `Key` = N'stats:succeeded'
) as s;
select sum(s.`Value`) from (
    select sum(`Value`) as `Value` from `{0}`.Counter where `Key` = N'stats:deleted'
    union all
    select `Value` from `{0}`.AggregatedCounter where `Key` = N'stats:deleted'
) as s;

select count(*) from `{0}`.`Set` where `Key` = N'recurring-jobs';
                ", schemaName);

            var stats = new StatisticsDto();
            using (var multi = connection.QueryMultiple(sql, commandTimeout: commandTimeout))
            {
                stats.Enqueued = multi.ReadSingle<int>();
                stats.Failed = multi.ReadSingle<int>();
                stats.Processing = multi.ReadSingle<int>();
                stats.Scheduled = multi.ReadSingle<int>();

                stats.Servers = multi.ReadSingle<int>();

                stats.Succeeded = multi.ReadSingleOrDefault<long?>() ?? 0;
                stats.Deleted = multi.ReadSingleOrDefault<long?>() ?? 0;

                stats.Recurring = multi.ReadSingle<int>();
            }

            return stats;
        }

        public static Dictionary<string, long> GetTimelineStats(
            IDbConnection connection, string schemaName, int? commandTimeout,
            ICollection<string> keys)
        {
            string sqlQuery =
$@"select `Key`, `Value` as `Count` from `{schemaName}`.AggregatedCounter
where `Key` in @keys";

            return connection.Query(
                sqlQuery,
                new { keys },
                commandTimeout: commandTimeout)
                .ToDictionary(x => (string)x.Key, x => (long)x.Count);
        }

        public static Dictionary<long, SqlJob> GetEnqueuedJobs(IDbConnection connection, string schemaName, int? commandTimeout, long[] jobIds)
        {
            string enqueuedJobsSql =
$@"select j.*, s.Reason as StateReason, s.Data as StateData 
from `{schemaName}`.Job j
left join `{schemaName}`.State s on s.Id = j.StateId and s.JobId = j.Id
where j.Id in @jobIds";

            return connection.Query<SqlJob>(
                enqueuedJobsSql,
                new { jobIds },
                commandTimeout: commandTimeout)
                .ToDictionary(x => x.Id, x => x);
        }

        public static long GetNumberOfJobsByStateName(IDbConnection connection, string schemaName, int? commandTimeout, string stateName, int? jobListLimit)
        {
            var sqlQuery = jobListLimit.HasValue
                ? $@"select count(j.Id) from (select top (@limit) Id from `{schemaName}`.Job where StateName = @state) as j"
                : $@"select count(Id) from `{schemaName}`.Job where StateName = @state";

            return connection.ExecuteScalar<int>(
                 sqlQuery,
                 new { state = stateName, limit = jobListLimit },
                 commandTimeout: commandTimeout); 
        }

        public static List<SqlJob> GetJobs(
            IDbConnection connection, string schemaName, int? commandTimeout,
            int from, int count, string stateName)
        {
            string jobsSql =
$@";with cte as 
(
  select j.Id, row_number() over (order by j.Id desc) as row_num
  from `{schemaName}`.Job j
  where j.StateName = @stateName
)
select j.*, s.Reason as StateReason, s.Data as StateData
from `{schemaName}`.Job j
inner join cte on cte.Id = j.Id 
left join `{schemaName}`.State s on j.StateId = s.Id and j.Id = s.JobId
where cte.row_num between @start and @end
order by j.Id desc";


            return connection.Query<SqlJob>(
                        jobsSql,
                        new { stateName, start = @from + 1, end = @from + count },
                        commandTimeout: commandTimeout)
                        .ToList();
        }

        public static List<SqlJob> GetFetchedJobs(IDbConnection connection, string schemaName, int? commandTimeout, IEnumerable<long> jobIds)
        {
            string fetchedJobsSql =
$@"select j.*, s.Reason as StateReason, s.Data as StateData 
from `{schemaName}`.Job j
left join `{schemaName}`.State s on s.Id = j.StateId and s.JobId = j.Id
where j.Id in @jobIds";
            
            return connection.Query<SqlJob>(
                fetchedJobsSql,
                new { jobIds },
                commandTimeout: commandTimeout)
                .ToList();
        }

        public static int GetActiveConnections(IDbConnection connection)
        {
            var sqlQuery = @"
select count(*) from sys.sysprocesses
where dbid = db_id(@name) and status != 'background' and status != 'sleeping'";
            return connection.Query<int>(sqlQuery, new { name = connection.Database }).Single();
        }

        public static int GetTotalConnections(IDbConnection connection)
        {
            var sqlQuery = @"
select count(*) from sys.sysprocesses
where dbid = db_id(@name) and status != 'background'";
            return connection.Query<int>(sqlQuery, new { name = connection.Database }).Single();
        }

        public static void RemoveFromQueue(IDbConnection connection, string schemaName, int? commandTimeout, long id)
        {
            connection.Execute(
                        $"delete from {schemaName}.JobQueue where Id = @id",
                        new { id },
                        commandTimeout: commandTimeout);
        }

        public static void Requeue(IDbConnection connection, string schemaName, int? commandTimeout, long id)
        {
            connection.Execute(
                        $"update {schemaName}.JobQueue set FetchedAt = null where Id = @id",
                        new { id },
                        commandTimeout: commandTimeout);
        }

        public static void ExecuteKeepAliveQuery(IDbConnection connection, string schemaName, int? commandTimeout, long id)
        {
            connection.Execute(
                            $"update {schemaName}.JobQueue set FetchedAt = getutcdate() where Id = @id",
                            new { id },
                            commandTimeout: commandTimeout);
        }

        public static void ExecuteKeepAliveQuery(IDbConnection connection, IDbTransaction transaction)
        {
            connection?.Execute("SELECT 1;", transaction);
        }

        public static string GetSetXactNoCountSql()
        {
            return "set xact_abort on;set nocount on;";
        }
        public static string GetLockedResourceSql()
        {
            return "exec GET_LOCK @Resource=@resource, @LockMode=N'Exclusive'";
        }
        public static string GetExpireJobSql(string schemaName)
        {
            return $@"update `{schemaName}`.Job set ExpireAt = @expireAt where Id = @id";
        }
        public static string GetPersistJobSql(string schemaName)
        {
            return $@"update `{schemaName}`.Job set ExpireAt = NULL where Id = @id";
        }
        public static string GetSetJobStateSql(string schemaName)
        {
            return $@"insert into `{schemaName}`.State (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data);
update `{schemaName}`.Job set StateId = SCOPE_IDENTITY(), StateName = @name where Id = @id;";
        }
        public static string GetAddJobStateSql(string schemaName)
        {
            return $@"insert into `{schemaName}`.State (JobId, Name, Reason, CreatedAt, Data)
values (@jobId, @name, @reason, @createdAt, @data)";
        }
        public static string GetCounterSql(string schemaName, bool hasExpireIn = false)
        {
            if (hasExpireIn)
            {
                return $@"insert into `{schemaName}`.Counter (`Key`, `Value`, `ExpireAt`) values (@key, @value, @expireAt)";
            }
            return $@"insert into `{schemaName}`.Counter (`Key`, `Value`) values (@key, @value)";
        }
        public static string GetAddToSetSql(string schemaName)
        {
            return $@";merge `{schemaName}`.`Set` as Target
using (VALUES (@key, @value, @score)) as Source (`Key`, Value, Score)
on Target.`Key` = Source.`Key` and Target.Value = Source.Value
when matched then update set Score = Source.Score
INSERT INTO `{schemaName}`.`Set`(`Key`, Value, Score) values (@key, @value, @score)
ON DUPLICATE KEY UPDATE Score = @score;";
        }
        public static string GetRemoveFromSetSql(string schemaName)
        {
            return $@"delete from `{schemaName}`.`Set` where `Key` = @key and Value = @value";
        }
        public static string GetInsertToListSql(string schemaName)
        {
            return $@"insert into `{schemaName}`.List (`Key`, Value) values (@key, @value);";
        }
        public static string GetRemoveFromListSql(string schemaName)
        {
            return $@"delete from `{schemaName}`.`List` where `Key` = @key and Value = @value";
        }
        public static string GetTrimListSql(string schemaName)
        {
            return $@";with cte as (
    select row_number() over (order by Id desc) as row_num
    from `{schemaName}`.List
    where `Key` = @key)
delete from cte where row_num not between @start and @end";
        }
        public static string GetSetRangeInHashSql(string schemaName)
        {
            return $@";merge `{schemaName}`.Hash as Target
using (VALUES (@key, @field, @value)) as Source (`Key`, Field, Value)
on Target.`Key` = Source.`Key` and Target.Field = Source.Field
when matched then update set Value = Source.Value
when not matched then insert (`Key`, Field, Value) values (Source.`Key`, Source.Field, Source.Value);";
        }
        public static string GetRemoveHashSql(string schemaName)
        {
            return $@"delete from `{schemaName}`.Hash where `Key` = @key";
        }
        public static string GetAddRangeToSetSql(string schemaName)
        {
            return $@"insert into `{schemaName}`.`Set` (`Key`, Value, Score)
values (@key, @value, 0.0)";
        }
        public static string GetRemoveSetSql(string schemaName)
        {
            return $@"delete from `{schemaName}`.Set where `Key` = @key";
        }
        public static string GetExpireHashSql(string schemaName)
        {
            return $@"update `{schemaName}`.`Hash` set ExpireAt = @expireAt where `Key` = @key";
        }
        public static string GetExpireSetSql(string schemaName)
        {
            return $@"update `{schemaName}`.`Set` set ExpireAt = @expireAt where `Key` = @key";
        }
        public static string GetExpireListSql(string schemaName)
        {
            return $@"update `{schemaName}`.`List` set ExpireAt = @expireAt where `Key` = @key";
        }
        public static string GetPersistHashSql(string schemaName)
        {
            return $@"update `{schemaName}`.`Hash` set ExpireAt = null where `Key` = @key";
        }
        public static string GetPersistSetSql(string schemaName)
        {
            return $@"update `{schemaName}`.`Set` set ExpireAt = null where `Key` = @key";
        }
        public static string GetPersistListSql(string schemaName)
        {
            return $@"update `{schemaName}`.`List` set ExpireAt = null where `Key` = @key";
        }

        public static string CreateExpiredJob(IDbConnection connection, string schemaName, int? commandTimeout,
            string invocationData,
            string arguments,
            DateTime createdAt,
            DateTime expireAt)
        {
            string createJobSql =
$@"insert into `{schemaName}`.Job (InvocationData, Arguments, CreatedAt, ExpireAt)
output inserted.Id
values (@invocationData, @arguments, @createdAt, @expireAt)";

            return connection.ExecuteScalar<long>(
                    createJobSql,
                    new
                    {
                        invocationData,
                        arguments,
                        createdAt,
                        expireAt
                    },
                    commandTimeout: commandTimeout).ToString();
        }
        public static string GetInsertParameterSql(string schemaName)
        {
            return $@"insert into `{schemaName}`.JobParameter (JobId, Name, Value)
values (@jobId, @name, @value)";
        }
        public static SqlJob GetJobData(IDbConnection connection, string schemaName, int? commandTimeout, long id)
        {
            string sql =
$@"select InvocationData, StateName, Arguments, CreatedAt from `{schemaName}`.Job  where Id = @id";

            return connection.Query<SqlJob>(sql, new { id }, commandTimeout: commandTimeout)
                    .SingleOrDefault();
        }

        public static SqlState GetStateData(IDbConnection connection, string schemaName, int? commandTimeout, long jobId)
        {
            string sql =
$@"select s.Name, s.Reason, s.Data
from `{schemaName}`.State s 
inner join `{schemaName}`.Job j  on j.StateId = s.Id and j.Id = s.JobId
where j.Id = @jobId";

            return connection.Query<SqlState>(sql, new { jobId }, commandTimeout: commandTimeout).SingleOrDefault();
        }

        public static int SetJobParameter(IDbConnection connection, string schemaName, int? commandTimeout, long jobId, string name, string value)
        {
            return connection.Execute(
$@";merge `{schemaName}`.JobParameter as Target
using (VALUES (@jobId, @name, @value)) as Source (JobId, Name, Value) 
on Target.JobId = Source.JobId AND Target.Name = Source.Name
when matched then update set Value = Source.Value
when not matched then insert (JobId, Name, Value) values (Source.JobId, Source.Name, Source.Value);",
                    new { jobId, name, value },
                    commandTimeout: commandTimeout);
        }

        public static string GetJobParameter(IDbConnection connection, string schemaName, int? commandTimeout, long id, string name)
        {
            return connection.ExecuteScalar<string>(
                $@"select top (1) Value from `{schemaName}`.JobParameter  where JobId = @id and Name = @name",
                new { id, name },
                commandTimeout: commandTimeout);
        }

        public static IEnumerable<string> GetAllItemsFromSet(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            return connection.Query<string>(
                    $@"select Value from `{schemaName}`.`Set`  where `Key` = @key",
                    new { key },
                    commandTimeout: commandTimeout);
        }

        public static string GetFirstByLowestScoreFromSet(IDbConnection connection, string schemaName, int? commandTimeout, string key, double fromScore, double toScore)
        {
            return connection.ExecuteScalar<string>(
                $@"select top 1 Value from `{schemaName}`.`Set`  where `Key` = @key and Score between @from and @to order by Score",
                new { key, from = fromScore, to = toScore },
                commandTimeout: commandTimeout);
        }
        public static string GetLockResourceKeySql()
        {
            return "SET XACT_ABORT ON;exec GET_LOCK @Resource=@resource, @LockMode=N'Exclusive', @LockOwner=N'Transaction', @LockTimeout=-1;";
        }
        public static string GetReleaseLockResourceKeySql()
        {
            return "exec RELEASE_LOCK @Resource=@resource, @LockOwner=N'Transaction';";
        }
        public static Dictionary<string, string> GetAllEntriesFromHash(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            return connection.Query<SqlHash>(
                    $"select Field, Value from `{schemaName}`.Hash where `Key` = @key",
                    new { key },
                    commandTimeout: commandTimeout)
                    .ToDictionary(x => x.Field, x => x.Value);
        }
        public static int AnnounceServer(IDbConnection connection, string schemaName, int? commandTimeout, string serverId, string data, DateTime heartbeat)
        {
            return connection.Execute(
$@";merge `{schemaName}`.Server as Target
using (VALUES (@id, @data, @heartbeat)) as Source (Id, Data, Heartbeat)
on Target.Id = Source.Id
when matched then update set Data = Source.Data, LastHeartbeat = Source.Heartbeat
when not matched then insert (Id, Data, LastHeartbeat) values (Source.Id, Source.Data, Source.Heartbeat);",
                    new { id = serverId, data, heartbeat },
                    commandTimeout: commandTimeout);
        }
        public static int RemoveServer(IDbConnection connection, string schemaName, int? commandTimeout, string serverId)
        {
            return connection.Execute(
                    $@"delete from `{schemaName}`.Server where Id = @id",
                    new { id = serverId },
                    commandTimeout: commandTimeout);
        }
        public static int Heartbeat(IDbConnection connection, string schemaName, int? commandTimeout, string serverId, DateTime now)
        {
            return connection.Execute(
                    $@"update `{schemaName}`.Server set LastHeartbeat = @now where Id = @id",
                    new { now, id = serverId },
                    commandTimeout: commandTimeout);
        }
        public static int RemoveTimedOutServers(IDbConnection connection, string schemaName, int? commandTimeout, DateTime timeOutAt)
        {
            return connection.Execute(
                $@"delete from `{schemaName}`.Server where LastHeartbeat < @timeOutAt",
                new { timeOutAt },
                commandTimeout: commandTimeout);
        }
        public static long GetSetCount(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            return connection.Query<int>(
                $"select count(`Key`) from `{schemaName}`.`Set`  where `Key` = @key",
                new { key },
                commandTimeout: commandTimeout).First();
        }
        public static List<string> GetRangeFromSet(IDbConnection connection, string schemaName, int? commandTimeout, string key, int startingFrom, int endingAt)
        {
            string query =
$@"select `Value` from (
	select `Value`, row_number() over (order by `Key` ASC) as row_num
	from `{schemaName}`.`Set` 
	where `Key` = @key 
) as s where s.row_num between @startingFrom and @endingAt";

            return connection
                .Query<string>(query, new { key, startingFrom, endingAt }, commandTimeout: commandTimeout)
                .ToList();
        }
        public static DateTime? GetSetTtl(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query = $@"select min(`ExpireAt`) from `{schemaName}`.`Set`  where `Key` = @key";

            return connection.ExecuteScalar<DateTime?>(query, new { key }, commandTimeout: commandTimeout);
        }
        public static long? GetCounter(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query =
$@"select sum(s.`Value`) from (select sum(`Value`) as `Value` from `{schemaName}`.Counter 
where `Key` = @key
union all
select `Value` from `{schemaName}`.AggregatedCounter 
where `Key` = @key) as s";

            return connection.ExecuteScalar<long?>(query, new { key }, commandTimeout: commandTimeout);
        }
        public static long GetHashCount(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query = $@"select count(*) from `{schemaName}`.Hash  where `Key` = @key";

            return connection.ExecuteScalar<long>(query, new { key }, commandTimeout: commandTimeout);
        }
        public static DateTime? GetHashTtl(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query = $@"select min(`ExpireAt`) from `{schemaName}`.Hash  where `Key` = @key";

            return connection.ExecuteScalar<DateTime?>(query, new { key }, commandTimeout: commandTimeout);
        }
        public static string GetValueFromHash(IDbConnection connection, string schemaName, int? commandTimeout, string key, string name)
        {
            string query =
$@"select `Value` from `{schemaName}`.Hash 
where `Key` = @key and `Field` = @field";

            return connection.ExecuteScalar<string>(query, new { key, field = name }, commandTimeout: commandTimeout);
        }
        public static long GetListCount(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query =
$@"select count(`Id`) from `{schemaName}`.List 
where `Key` = @key";

            return connection.ExecuteScalar<long>(query, new { key }, commandTimeout: commandTimeout);
        }

        public static DateTime? GetListTtl(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            if (key == null) throw new ArgumentNullException(nameof(key));

            string query =
$@"select min(`ExpireAt`) from `{schemaName}`.List 
where `Key` = @key";

            return connection.ExecuteScalar<DateTime?>(query, new { key }, commandTimeout: commandTimeout);
        }

        public static List<string> GetRangeFromList(IDbConnection connection, string schemaName, int? commandTimeout, string key, int startingFrom, int endingAt)
        {
            string query =
$@"select `Value` from (
	select `Value`, row_number() over (order by `Id` desc) as row_num 
	from `{schemaName}`.List 
	where `Key` = @key 
) as s where s.row_num between @startingFrom and @endingAt";

            return connection
                .Query<string>(query, new { key, startingFrom, endingAt }, commandTimeout: commandTimeout)
                .ToList();
        }

        public static List<string> GetAllItemsFromList(IDbConnection connection, string schemaName, int? commandTimeout, string key)
        {
            string query =
$@"select `Value` from `{schemaName}`.List 
where `Key` = @key
order by `Id` desc";

            return connection.Query<string>(query, new { key }, commandTimeout: commandTimeout).ToList();
        }
    }
}
