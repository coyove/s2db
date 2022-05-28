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
- `WeakCacheSize (int)`: weak cache size (number of weakly cached objects)
- `CacheObjMaxSize (int, kilobytes)`: max allowed size of a cached object
- `SlowLimit (int, milliseconds)`: threshold of recording slow commands into ./log/slow.log
- `ResponseLogSize (int, bytes)`: max size of logs master can push to slave in PUSHLOGS
- `BatchMaxRun (int)`: batch operations size
- `BatchFirstRunSleep (int, milliseconds)`: time window of grouping continuous deferred commands 
- `CompactLogsTTL (int, seconds)`: TTL of logs
- `DisableMetrics (int, 0|1)`: disable internal metrics recording
- `InspectorSource (string)`: internal script code, see [self-managed code](#Self-managed%20Code)

# Commands
### DEL key [key2]
    Delete 'key'.
        If 'key2' is specified, all keys between [key, key2] will be deleted. Range deletion is a heavy command, use with cautions.
### ZADD key [--DEFER--|--SYNC--] [NX|XX] [CH] [PD] [DSLT dslt_score] score member [score member ...]
    --DEFER--:
        The operation will be queued up and executed later, server will return 'OK' immediately instead of the actual result of this execution.
        s2db will group continuous deferred operations together for better performance, so caller should use '--DEFER--' whenever possible.
        This flag must be positioned right after 'key'.
    --SYNC--:
        If slave exists, the operation will only succeed when slave also acknowledges successfully, thus strong consistency is guaranteed.
        If slave fails to respond within 'PingTimeout', master will return a timeout error (to caller) even master itself succeeded.
        This flag must be positioned right after 'key'.
    DSLT:
        If provided, all members (including existing members in 'key') with scores less than 'dslt_score' will be deleted.
    PD:
        Don't overwrite data if members existed already, e.g.: ZADD zset DATA 1 hello world
            this command updates 'hello' score to 2, but also overwrites 'world':
                ZADD zset 2 hello
            this command preserves 'world' while updating its score to 2:
                ZADD zset pd 2 hello 
### ZADD key DATA score member data [score member data ...]
    Behaves similar to the above command, but you can attach data to each member. All flags of the original ZADD can be used along with.
### Z[M]DATA key member [member ...]
    Retrieve the data attached to the members.
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
### EVAL code [arg0 [arg1 ...]]
    Evaluate the code with provided arguments, which are stored in `args` variable.

# Weak Cache
Read commands like `ZRANGE` or `ZSCORE` will store results in a weak cache.
These cached values will not be returned to clients unless they append `WEAK sec` to commands,
e.g.: `ZRANGE key start end WEAK 30` means returning cached results of this command if it was just cached in less than 30 seconds.

# Web Console
Web console can be accessed at the same address as flag `-l` identified, e.g.: `http://127.0.0.1:6379` and `http://127.0.0.1:6379/debug/pprof/`.

# Built-in Scripts
s2db use a Lua dialect called 'nj' as its script engine, to learn more, refer to this [repo](https://github.com/coyove/nj).

# Self-managed Code
By implementing certain functions in `InspectorSource`, s2db users (like devops) can manipulate database directly and internally:
1. `function cronjob30()`: the function will be called roughly every 30 seconds.
2. `function cronjob60()`: the function will be called roughly every 60 seconds.
3. `function cronjob300()`: the function will be called roughly every 300 seconds.

# Redirected 3rd-party Lib
1. https://github.com/secmask/go-wire