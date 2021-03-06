# s2db
s2db is a sorted set database who speaks redis protocol and stores data on disk.

# Startup Sequence
1. Start slave server by `s2db -l <SlaveIp>:<SlavePort> -C0 servername=<SlaveName>`, listening at `SlaveIp:SlavePort`.
2. Start master server by `s2db -l <Ip>:<Port> -C0 slave=<SlaveIp>:<SlavePort>/?Name=<SlaveName>`, listening at `Ip:Port`.

# Configuration Fields
- `ServerName (string)`: server's name
- `Password (string)`: server's password
- `Slave (string)`: slave's conn string, minimal form: `<SlaveIp>:<SlavePort>/?Name=<SlaveName>`:
- `MarkMaster (int, 0|1)`: mark server as master, rejecting all PUSHLOGS requests
- `Passthrough (string)`: relay all read-write commands to the destinated endpoint, minimal form: `<Ip>:<Port>/?Name=<Name>`:
- `PingTimeout (int)`: ping timeout of slave, used by master
- `CacheSize (int)`: cache size (number of cached objects)
- `WeakCacheSize (int)`: weak cache size
- `CacheObjMaxSize (int, kilobytes)`: max allowed size of a cached object
- `SlowLimit (int, milliseconds)`: threshold of recording slow commands into ./log/slow.log
- `ResponseLogRun (int)`: max number of logs master can push to slave in PUSHLOGS
- `ResponseLogSize (int, bytes)`: max size of logs master can push to slave in PUSHLOGS
- `BatchMaxRun (int)`: batch operations size, bigger value makes `--defer--` faster
- `CompactJobType (int)`: compaction job type, see [compaction](#compaction)
- `CompactLogHead (int)`: number of logs preserved during compaction
- `CompactTxSize (int)`: max transaction size during compaction
- `CompactTmpDir (string)`: temporal location for the compacted database, see [compaction](#compaction)
- `CompactTxWorkers (int)`: number of workers used to compact a shard
- `CompactNoBackup (int, 0|1)`: don't backup the old database after compaction
- `DisableMetrics (int, 0|1)`: disable internal metrics recording
- `InspectorSource (string)`: internal script code, see [self-managed code](#Self-managed%20Code)

# Commands
### DEL key
    Delete one key (ONE key only). Big zsets and queues (with >65536 members) can only be deleted using `UNLINK`.
### UNLINK key
    Unlink the key, which will get deleted during compaction.
    Unlinking will introduce unconsistency when slave and master have different compacting time windows,
    caller must make sure that unlinked keys will never be used and useful again.
### ZADD key [--DEFER--|--SYNC--] [NX|XX] [CH] [FILL fillpercent] score member [score member ...]
    --DEFER--:
        The operation will be queued up and executed later, server will return 'OK' immediately instead of the actual result of this execution.
        s2db will group continuous deferred operations together for better performance, so caller should use '--DEFER--' whenever possible.
    --SYNC--:
        If slave exists, the operation will only succeed when slave also acknowledges successfully, thus strong consistency is guaranteed.
        If slave fails to respond within 'PingTimeout', master will return a timeout error (to caller) even master itself succeeded.
    FILL:
        Value can range from 0.0 to 1.0 (default 0.5).
        If added members and their scores are both monotonically increasing, set FILL to a higher value to achieve better disk space utilization.
        A good example is adding snowflake IDs with timestamps:
            ZADD key FILL 0.9 timestamp1 id1 timestamp2 id2 ...
### ZADD key [--DEFER--|--SYNC--] [NX|XX] [CH] DATA score member data [score member data ...]
    Behaves similar to the above command, but you can attach data to each member.
### Z[M]DATA key member [member ...]
    Retrieve the data attached to the members.
### ZADD key longitude,latitude member
    Equivalent of GEOADD, e.g.: ZADD cities 118.76667,32.049999 Nanjing
### ZINCRBY key [--DEFER--|--SYNC--] increment memebr [datafunc]
    Behaves exactly like redis.
### ZREM key [--DEFER--|--SYNC--] member [member ...]
    Behaves exactly like redis.
### ZREMRANGEBY[LEX|SCORE|RANK] [--DEFER--|--SYNC--] key left right
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
### Z[REV]RANGE(BYLEX|BYSCORE) key left right [LIMIT 0 count] [WITHSCORES] [WITHDATA] [INTERSECT key] [NOTINTERSECT key] [TWOHOPS endpoint]
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

# Compaction
To enable compaction, execute: `CONFIG SET CompactJobType <Type>`, where `<Type>` can be (`hh` ranges `00-23`, `mm` ranges `00-59`):
- `0`: Disabled.
- `1hh`: Compaction starts at hh:00 UTC+0 everyday.
- `1hhmm`: Compaction starts at hh:mm UTC+0 everyday.
- `2hh`: Compaction starts at hh:00 UTC+0 everyday, the process will take place every 30 min, compacting shard from #0 to #31 sequenitally.
- `6mm`: Compaction starts at unix epoch + `mm` minutes, the process will take place every hour like above.

Compaction process reqiures enough disk space to store the temporal compacted database file (of a single shard). When done, s2db will use the compacted one to replace the online shard, during which all writes to this shard will be hung up.

Compaction consumes a lot of disk resources, by default it is written to the data directory, therefore, same device as the online one. To minimize the impaction to online requests and maximize the compacting speed, you can set a temporal dump location on another device by setting `CompactTmpDir`.

# Built-in Scripts
s2db use a Lua dialect called 'nj' as its script engine, to learn more, refer to this [repo](https://github.com/coyove/nj).

# Self-managed Code
By implementing certain functions in `InspectorSource`, s2db users (like devops) can manipulate database directly and internally:
1. `function cronjob30()`: the function will be called roughly every 30 seconds.
2. `function cronjob60()`: the function will be called roughly every 60 seconds.
3. `function cronjob300()`: the function will be called roughly every 300 seconds.
4. `function queuettl(queuename)`: return how old (seconds) a member in a queue can be preserved during compaction, 0 means no expiration.
5. `function compactonstart(shard)`: fired when each shard starts compacting.
6. `function compactonfinish(shard)`: fired when each shard finishes compacting.
7. `function compactonerror(shard, error)`: fired when compacting encountered error.
8. `function compactnotfinished(shard)`: fired when compacting not finished properly.

# Redirected 3rd-party Libs
1. https://github.com/etcd-io/bbolt
2. https://github.com/secmask/go-redisproto
3. https://github.com/huichen/sego