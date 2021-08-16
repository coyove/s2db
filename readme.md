# Startup Sequence
## Master
1. Start the server by `s2db -l <Ip>:<Port> -M`, listening at `Ip:Port`.
2. Connect to the server using any redis client: `redis-cli -p <Port>`.
3. Set server name: `CONFIG SET servername <ServerName>`.

## Slave
1. Start the server by `s2db -l <Ip>:<Port> -master <MasterIp>:<MasterPort>`, listening at `Ip:Port`.
2. `<MasterName>` must correspond to the actual master server's name you set earlier, otherwise no replication will happen.
3. Set server name like master did.
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
- `FillPercent`: database filling rate, 1~10 will be translated to 0.1~1.0 and 0 means bbolt default (0.5)
- `StopLogPull`: stop pulling logs from master

# Commands
## `DEL key`
Delete one key (ONE key only).

## `ZADD key [--DEFER--] [NX|XX] [CH] score member [score member ...]`
Behaviour exactly like redis. `--DEFER--` will make the addition asynchronized so the command will return `OK` immediately.

## `ZADD key [--DEFER--] [NX|XX] [CH] DATA score member data [score member data ...]`
Behaviour similar to the above command, but you can attach data to each member.

## `ZINCRBY key increment memebr`
Behaviour exactly like redis.

## `ZREM key member [member ...]`
Behaviour exactly like redis.

## `ZSCORE key member`
Behaviour exactly like redis.

## `ZMSCORE key member [member ...]`
Behaviour exactly like redis.

## `ZMDATA key member [member ...]`
Retrieve the data attached to the members.