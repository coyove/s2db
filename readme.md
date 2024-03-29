# s2db
s2db is a sorted set database who speaks redis protocol and stores data on disk.

# Startup Sequence
1. Start slave server by `s2db -l <SlaveIp>:<SlavePort> -C0 servername=<SlaveName>`, listening at `SlaveIp:SlavePort`.
2. Start master server by `s2db -l <Ip>:<Port> -C0 slave=<SlaveIp>:<SlavePort>/?Name=<SlaveName>`, listening at `Ip:Port`.

# Configuration Fields
- `ServerName (string)`: server's name
- `Password (string)`: server's password
- `Slave (string)`: slave's conn string, minimal form: `<SlaveIp>:<SlavePort>/?Name=<SlaveName>`:
- `PullMaster (string)`: master's conn string, minimal form: `<MasterIp>:<MasterPort>/?Name=<MasterName>`:
- `MarkMaster (int, 0|1)`: mark server as master, rejecting all PUSHLOGS requests
- `Passthrough (string)`: relay all read-write commands to the destinated endpoint, minimal form: `<Ip>:<Port>/?Name=<Name>`:
- `PingTimeout (int, milliseconds)`: ping timeout of slave, used by master
- `CacheSize (int)`: cache size (number of cached objects)
- `CacheObjMaxSize (int, kilobytes)`: max allowed size of a cached object, -1 means no object can be cached
- `SlowLimit (int, milliseconds)`: threshold of recording slow commands into ./log/slow.log
- `ResponseLogSize (int, bytes)`: max size of logs master can push to slave in PUSHLOGS
- `BatchMaxRun (int)`: batch operations size
- `BatchFirstRunSleep (int, milliseconds)`: time window of grouping continuous deferred commands 
- `CompactLogsTTL (int, seconds)`: TTL of logs
- `MetricsEndpoint (string)`: address of metrics collector, null means recording to internal metrics only

# Commands
Refer to COMMANDS.txt.

# Web Console
Web console can be accessed at the same address as flag `-l` identified, e.g.: `http://127.0.0.1:6379` and `http://127.0.0.1:6379/debug/pprof/`.

# Built-in Scripts
s2db use a Lua dialect called `nj` as its script engine, to learn more, refer to this [repo](https://github.com/coyove/nj).

# Redirected 3rd-party Lib
1. https://github.com/secmask/go-redisproto
