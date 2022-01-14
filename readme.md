# s2db
s2db is a sorted set database who speaks redis protocol and stores data on disk.

# Startup Sequence
## Master
1. Start the server by `s2db -l <Ip>:<Port> -M`, listening at `Ip:Port`.
2. Connect to the server using any redis client: `redis-cli -p <Port>`.
3. Set server name: `CONFIG SET servername <ServerName>`.

## Slave
1. Start the server by `s2db -l <Ip>:<Port> -master <MasterName>@<MasterIp>:<MasterPort>`, listening at `Ip:Port`.
2. `<MasterName>` must correspond to the actual master server's name you set earlier, otherwise no replication will happen.
3. Set slave's server name like master did.
4. Replications are done asynchronously and passively, which means master won't request any info from slaves.

# Configuration Fields
- `ServerName`: server's name
- `CacheSize`: memory cache size in megabytes
- `CacheKeyMaxLen`: max cached results per key
- `WeakCacheSize`: weak memory cache size in megabytes
- `SlowLimit`: record slow log running longer than X ms
- `ResponseLogRun`: max log entries replied by the master
- `ResponseLogSize`: max size (bytes) of logs replied by the master
- `BatchMaxRun`: batch operations size
- `SchedCompactJob`: scheduled compaction job config
- `CompactLogHead`: number of log entries preserved during compaction
- `CompactTxSize`: max transaction size during compaction
- `CompactTmpDir`: temporal location for the compacted database, by default it is located in the data directory
- `FillPercent`: database filling rate, 1~10 will be translated to 0.1~1.0 and 0 means bbolt default (0.5)
- `StopLogPull`: stop pulling logs from master

# Commands
`DEL key`

Delete one key (ONE key only).

`ZADD key [--DEFER--] [NX|XX] [CH] score member [score member ...]`

Behaves exactly like redis. `--DEFER--` makes the operation deferred so the command will return `OK` immediately.

`ZADD key [--DEFER--] [NX|XX] [CH] DATA score member data [score member data ...]`

Behaves similar to the above command, but you can attach data to each member.

`ZINCRBY key increment memebr`

Behaves exactly like redis.

`ZREM key member [member ...]`

Behaves exactly like redis.

`ZREMRANGEBY[LEX|SCORE|RANK] key left right`

Behaves exactly like redis.

`ZCARD key`

Behaves exactly like redis.

`ZCARDMATCH pattern`

Count the sum of the cardinality of all zsets who match `pattern`.

`ZCOUNT key min max`

Behaves exactly like redis.

`ZCOUNTLEX key min max`

Behaves similar to the above command, but sorts lexicographically.

`Z[M]SCORE key member`

Behaves exactly like redis.

`ZMDATA key member [member ...]`

Retrieve the data attached to the members.

`Z[REV]RANK key member`

Behaves exactly like redis.

`ZRANGEBY[LEX|SCORE] key left right [LIMIT 0 count]`

Behaves similar to redis, except `offset` (if provided) must be 0.

`GEODIST key member1 member2 [m|km] `

Behaves exactly like redis.

`GEOPOS key member [member ...]`

Behaves exactly like redis.

`GEORADIUS[_RO] key longitude latitude radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]`

Readonly command and behaves exactly like redis.

`GEORADIUSBYMEMBER[_RO] key member radius m|km [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC]`

Readonly command and behaves exactly like redis.

`ZADD cities 118.76667,32.049999 Nanjing`

Equivalent of `GEOADD` in s2db.

# Compaction
To enable compaction, execute: `CONFIG SET CompactJobType <Type>`, where `<Type>` can be (`hh` ranges `00-23`, `mm` ranges `00-59`):
- `0`: Disabled.
- `1hh`: Compaction starts at hh:00 UTC+0 everyday.
- `1hhmm`: Compaction starts at hh:mm UTC+0 everyday.
- `2hh`: Compaction starts at hh:00 UTC+0 everyday, shard compaction will take place every 30 min, from #0 to #31.
- `6mm`: Compaction starts at unix epoch + `mm` minutes, shard compaction will take place every hour, from #0 to #31.

Compactions are done in the background without hanging the incoming requests, this reqiures enough disk space to store the temporal compacted database file. When done compacting, s2db will use this file to replace the online one, causing denial of service temporarily (writes only, reads won't be affected).

Compaction consumes a lot of disk resources. By default it is written to the data directory, therefore, same device as the online one, to minimize the impaction to online requests and maximize the compacting speed, you can set a temporal location on another device for s2db to dump the compacted database into by setting `CompactTmpDir`.
