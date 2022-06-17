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
- `MetricsEndpoint (string)`: address of metrics collector, null means recording to internal metrics only

# Commands
Refer to COMMANDS.txt.

# Weak Cache
Read commands like `ZRANGE` or `ZSCORE` will store results in a weak cache.
These cached values will not be returned to clients unless they append `WEAK sec` to commands,
e.g.: `ZRANGE key start end WEAK 30` means returning cached results of this command if it was just cached in less than 30 seconds.

# Web Console
Web console can be accessed at the same address as flag `-l` identified, e.g.: `http://127.0.0.1:6379` and `http://127.0.0.1:6379/debug/pprof/`.

# Built-in Scripts
s2db use a Lua dialect called `nj` as its script engine, to learn more, refer to this [repo](https://github.com/coyove/nj).

# Redirected 3rd-party Lib
1. https://github.com/secmask/go-redisproto
