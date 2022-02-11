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
- `ServerName`: server's name
- `CacheSize`: memory cache size in megabytes
- `CacheKeyMaxLen`: max cached results per key
- `WeakCacheSize`: weak memory cache size in megabytes
- `SlowLimit`: record slow log running longer than X ms
- `ResponseLogRun`: max log entries replied by the master
- `ResponseLogSize`: max size (bytes) of logs replied by the master
- `BatchMaxRun`: batch operations size
- `CompactJobType`: compaction job type
- `CompactLogHead`: number of log entries preserved during compaction
- `CompactTxSize`: max transaction size during compaction
- `CompactTmpDir`: temporal location for the compacted database, by default it is located in the data directory
- `CompactTxWorkers`: number of workers used to compact a shard
- `CompactNoBackup`: don't backup the old database after compaction
- `StopLogPull`: stop pulling logs from master

# Commands
```bash
DEL key
    # Delete one key (ONE key only).
    # Big zsets and queues (with more than 65536 members) can only be deleted using 'UNLINK'.

UNLINK key
    # Unlink the key, which will get deleted during compaction.
	# Unlinking will introduce unconsistency when slave and master have different compacting time windows,
    # caller must make sure that unlinked keys will never be used and useful again.

ZADD key [--DEFER--] [NX|XX] [CH] [FILL fill_percent] score member [score member ...]
    # Behaves exactly like redis. '--DEFER--' makes the operation deferred so the command will return 'OK' immediately.

ZADD key [--DEFER--] [NX|XX] [CH] DATA score member data [score member data ...]
    # Behaves similar to the above command, but you can attach data to each member.

ZADD key longitude,latitude member
    # Equivalent of 'GEOADD' e.g.: ZADD cities 118.76667,32.049999 Nanjing

ZINCRBY key increment memebr
ZREM key member [member ...]
ZREMRANGEBY[LEX|SCORE|RANK] key left right
ZCARD key
ZCOUNT key min max
Z[M]SCORE key member
Z[REV]RANK key member
Z[REV]RANGE key start end
GEODIST key member1 member2 [m|km]
GEOPOS key member [member ...]
GEORADIUS[_RO] key longitude latitude radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
GEORADIUSBYMEMBER[_RO] key member radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]
    # Behaves exactly like redis.

ZCOUNTLEX key min max [MATCH pattern]
    # Behaves similar to 'ZCOUNT', but sorts lexicographically.

ZMDATA key member [member ...]
    # Retrieve the data attached to the members.

ZRANGE(BYLEX|BYSCORE) key left right [LIMIT 0 count] [WITHSCORES] [WITHDATA] [INTERSECT key2 [INTERSECT key3 ...]] [MERGE key2 [MERGE key3 ...]] [MERGEFUNC code] [TWOHOPS endpoint]
    # Behaves similar to redis, except that 'offset' in LIMIT must be 0 if provided.
    # INTERSECT: returned members will exist in every key: 'key', 'key2', ... 
    # MERGE: for every member in 'key', merge its score with other scores in 'key2', 'key3', ...
    # TWOHOPS: consider every member in 'key' pointes to a zset with the same name, find those members who contains 'endpoint' (ZSCORE member endpoint)

SCAN cursor [SHARD shard] [MATCH pattern] [COUNT count]
    # Scan keys in database (or in a particular shard).
```

# Weak Cache
Read commands like `ZRANGE` or `ZMDATA` will store results into a weak cache. Cached values will not be returned in the preceding requests unless you append `WEAK sec` to your commands, e.g.: `ZRANGE key start end WEAK 30` means returning cached results of `ZRANGE` if they are younger than 30 seconds.

# Web Console
Web console can be accessed at the same address as flag `-l` identified, e.g.: `http://127.0.0.1:6379`. To disable it, use flag `-no-web`.

# Compaction
To enable compaction, execute: `CONFIG SET CompactJobType <Type>`, where `<Type>` can be (`hh` ranges `00-23`, `mm` ranges `00-59`):
- `0`: Disabled.
- `1hh`: Compaction starts at hh:00 UTC+0 everyday.
- `1hhmm`: Compaction starts at hh:mm UTC+0 everyday.
- `2hh`: Compaction starts at hh:00 UTC+0 everyday, the process will take place every 30 min, compacting shard from #0 to #31 sequenitally.
- `6mm`: Compaction starts at unix epoch + `mm` minutes, the process will take place every hour like above.

Compactions are done in the background which reqiures enough disk space to store the temporal compacted database file. When done compacting, s2db will use this file to replace the online one, during which all writes will be hung up.

Compaction consumes a lot of disk resources, by default it is written to the data directory, therefore, same device as the online one. To minimize the impaction to online requests and maximize the compacting speed, you can set a temporal dump location on another device by setting `CompactTmpDir`.
