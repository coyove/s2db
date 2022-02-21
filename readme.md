# s2db
s2db is a sorted set database who speaks redis protocol and stores data on disk.

# Startup Sequence
## Master
1. Start the server by `s2db -l <Ip>:<Port> -M`, listening at `Ip:Port`.
2. Connect to the server using any redis client: `redis-cli -p <Port>`.
3. Set server name: `CONFIG SET servername <ServerName>`, otherwise writes will be omitted.

## Slave
1. Start the server by `s2db -l <Ip>:<Port> -master <MasterName>@<MasterIp>:<MasterPort>`, listening at `Ip:Port`.
2. `<MasterName>` must correspond to the actual master server's name you set earlier, otherwise no replication will happen.
3. Set slave's server name like master did.
4. Replications are done asynchronously and passively, master won't request any info from slaves.

# Configuration Fields
- `ServerName (string)`: server's name
- `CacheSize (int)`: cache size (number of cached objects)
- `WeakCacheSize (int)`: weak cache size
- `CacheObjMaxSize (int, kilobytes)`: max allowed size of a cached object
- `SlowLimit (int, milliseconds)`: threshold of recording slow commands into ./log/slow.log
- `ResponseLogRun (int)`: max number of logs master can return to slaves
- `ResponseLogSize (int, bytes)`: max size of logs master can return to slaves
- `BatchMaxRun (int)`: batch operations size, bigger value makes `--defer--` faster
- `CompactJobType (int)`: compaction job type, see [compaction](#compaction)
- `CompactLogHead (int)`: number of logs preserved during compaction
- `CompactTxSize (int)`: max transaction size during compaction
- `CompactTmpDir (string)`: temporal location for the compacted database, see [compaction](#compaction)
- `CompactTxWorkers (int)`: number of workers used to compact a shard
- `CompactNoBackup (int, 0|1)`: don't backup the old database after compaction
- `StopLogPull (int, 0|1)`: stop pulling logs from master
- `InspectorSource (string)`: internal script code, see [self-managed code](#Self-managed%20Code)

# Commands
### DEL key
    Delete one key (ONE key only). Big zsets and queues (with >65536 members) can only be deleted using `UNLINK`.
### UNLINK key
    Unlink the key, which will get deleted during compaction.
    Unlinking will introduce unconsistency when slave and master have different compacting time windows,
    caller must make sure that unlinked keys will never be used and useful again.
### ZADD key [--DEFER--] [NX|XX] [CH] [FILL fillpercent] score member [score member ...]
    --DEFER--:
        The operation will be queued up and executed later, server will return 'OK' immediately, the actual result will be dropped.
        Multiple deferred operations may be grouped together for better performance.
    FILL:
        Value can range from 0.0 to 1.0 (default 0.5).
        If added members and their scores are both monotonically increasing, set FILL to a higher value to achieve better disk space utilization.
        A good example is adding snowflake IDs with timestamps:
            ZADD key FILL 0.9 timestamp1 id1 timestamp2 id2 ...
### ZADD key [--DEFER--] [NX|XX] [CH] DATA score member data [score member data ...]
    Behaves similar to the above command, but you can attach data to each member.
### ZMDATA key member [member ...]
    Retrieve the data attached to the members.
### ZADD key longitude,latitude member
    Equivalent of GEOADD, e.g.: ZADD cities 118.76667,32.049999 Nanjing
### ZINCRBY key [--DEFER--] increment memebr
    Behaves exactly like redis.
### ZREM key [--DEFER--] member [member ...]
    Behaves exactly like redis.
### ZREMRANGEBY[LEX|SCORE|RANK] [--DEFER--] key left right
    Behaves exactly like redis.
### ZCARD key
    Behaves exactly like redis.
### ZCOUNT key min max
    Behaves exactly like redis.
### ZCOUNTLEX key min max [MATCH pattern]
    Behaves similar to ZCOUNT, but sorts lexicographically.
### Z[M]SCORE key member
    Behaves exactly like redis.
### Z[REV]RANK key member
    Behaves exactly like redis.
### Z[REV]RANGE key start end
    Behaves exactly like redis.
### GEODIST key member1 member2 [m|km]
    Behaves exactly like redis.
### GEOPOS key member [member ...]
    Behaves exactly like redis.
### GEORADIUS[_RO] key longitude latitude radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
    Behaves exactly like redis, the command is always read-only.
### GEORADIUSBYMEMBER[_RO] key member radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
    Behaves exactly like redis, the command is always read-only.
### Z[REV]RANGE(BYLEX|BYSCORE) key left right [LIMIT 0 count] [WITHSCORES] [WITHDATA] [INTERSECT key] [NOTINTERSECT key] [MERGE key [MERGEFUNC code]] [TWOHOPS endpoint]
    LIMIT:
        First argument (offset) must be 0 if provided.
    INTERSECT:
        Returned members must exist in all INTERSECT keys:
            ZRANGEBYLEX key - + INTERSECT key2 INTERSECT key3
    NOTINTERSECT:
        Returned members must not exist in all NOTINTERSECT keys:
            ZRANGEBYLEX key - + NOTINTERSECT key2 NOTINTERSECT key3
        Can be used along with INTERSECT:
            ZRANGEBYLEX key - + INTERSECT key2 NOTINTERSECT key3
    MERGE/MERGEFUNC:
        For every member in 'key', merge its score with scores in MERGE keys, summation will be used if MERGEFUNC is empty:
            ZRANGEBYLEX key - + MERGE key2 MERGE key3
            ZRANGEBYLEX key - + MERGE key2 MERGEFUNC "lambda(member, scores) scores[0] + scores[1] end"
    TWOHOPS:
        Returned members each pointes to a zset with the same name, all these zsets must contain TWOHOPS endpoint.
            ZADD key2 score endpoint
            ZADD key score key2
            ZRANGEBYLEX key - + TWOHOPS endpoint # results: [key2]
### SCAN cursor [SHARD shard] [MATCH pattern] [COUNT count]
    Scan keys in database (or in a particular shard).
### QAPPEND queue member [COUNT queue_max_size] [MATCH code]
    Append 'member' to 'queue', return the index in queue (starting from 1).
    COUNT:
        Limit the size of queue, older members will be dropped to meet this requirement.
        Set 'member' to '--TRIM--' to avoid adding new members to the queue, while still triming the queue to desired size.
    MATCH:
        Append 'member' to the queue only when 'code' returns true:
            QAPPEND q first
                Append 'first' to 'q'.
            QAPPEND q first MATCH "lambda(prev) prev != 'first' end"
                Can't append 'first' (twice) to 'q'.
### QSCAN queue cursor count [WITHSCORES] [WITHDATA]
    Scan the queue from 'cursor' and return 'count' members at most. If 'count' is negative, scanning will travel backward (from newest to oldest).
    Cursor can be one of the following forms:
        1. Relative cursor:
            QSCAN q 2 10
                Scan 'q' starting from the second (oldest) member, return 10 members
            QSCAN q 0 -10
                Scan 'q' backward, starting from the last (newest) member, return 10 members
            QSCAN q -1 -10
                Scan 'q' backward, starting from the second to last member, return 10 members
        2. Absolute cursor:
            QSCAN q !2 10
                Scan 'q' starting at index 2, return 10 members. If index 2 has been trimmed (say the oldest index is 3), no members will be returned.
        3. Timestamp Cursor:
            QSCAN q @1600000000000000000 10
                Scan 'q' starting at 2020-09-13T12:26:40, return 10 members. Timestamp must be nanoseconds since unix epoch.
    WITHSCORES:
        Each member's index in queue will be returned as its score.
    WITHDATA:
        Each member's index in queue will be returned as its score.
        Each member's timestamp will be returned as its data (nanoseconds formatted to decimal string).
### QLEN queue
    Return the length of 'queue'.
### EVAL code [arg0 [arg1 ...]]
    Evaluate the code with provided arguments, which are stored in `args` variable.

# Weak Cache
Read commands like `ZRANGE` or `ZSCORE` will store results in a weak cache.
These cached values will not be returned to clients unless they append `WEAK sec` to commands,
e.g.: `ZRANGE key start end WEAK 30` means returning cached results of this command if it was just cached in less than 30 seconds.

# Web Console
Web console can be accessed at the same address as flag `-l` identified, e.g.: `http://127.0.0.1:6379` and `http://127.0.0.1:6379/debug/pprof/`.
To disable it, use flag `-no-web-console`.

# Compaction
To enable compaction, execute: `CONFIG SET CompactJobType <Type>`, where `<Type>` can be (`hh` ranges `00-23`, `mm` ranges `00-59`):
- `0`: Disabled.
- `1hh`: Compaction starts at hh:00 UTC+0 everyday.
- `1hhmm`: Compaction starts at hh:mm UTC+0 everyday.
- `2hh`: Compaction starts at hh:00 UTC+0 everyday, the process will take place every 30 min, compacting shard from #0 to #31 sequenitally.
- `6mm`: Compaction starts at unix epoch + `mm` minutes, the process will take place every hour like above.

Compactions are done in the background which reqiures enough disk space to store the temporal compacted database file. When done compacting, s2db will use this file to replace the online one, during which all writes will be hung up.

Compaction consumes a lot of disk resources, by default it is written to the data directory, therefore, same device as the online one. To minimize the impaction to online requests and maximize the compacting speed, you can set a temporal dump location on another device by setting `CompactTmpDir`.

# Built-in Scripts
s2db use a lua-dialect called 'nj' as its script engine, to learn more, refer to this [repo](https://github.com/coyove/nj).

# Self-managed Code
By implementing certain functions in `InspectorSource`, s2db users (like devops) can manipulate database directly and internally:
1. `function cronjob30()`: the function will be called roughly every 30 seconds.
2. `function cronjob60()`: the function will be called roughly every 60 seconds.
3. `function cronjob300()`: the function will be called roughly every 300 seconds.
4. `function queuettl(queuename)`: return how old (seconds) a member in a queue can be preserved during compaction, 0 means no expiration.
5. `function compactonstart(shard)`: fired when each shard starts compacting.
6. `function compactonfinish(shard)`: fired when each shard finishes compacting.
7. `function compactonresume(shard)`: fired when a shard resumes compacting.