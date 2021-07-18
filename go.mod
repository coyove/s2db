module gitlab.litatom.com/zhangzezhong/zset

go 1.16

require (
	github.com/coyove/common v0.0.0-20210601082815-1e7f2ca0bb84
	github.com/go-redis/redis/v8 v8.11.0
	github.com/mitchellh/go-ps v1.0.0
	github.com/secmask/go-redisproto v0.1.0
	github.com/sirupsen/logrus v1.8.1
	go.etcd.io/bbolt v1.3.6
	gopkg.in/natefinch/lumberjack.v2 v2.0.0
	gopkg.in/yaml.v2 v2.4.0

)

replace github.com/secmask/go-redisproto v0.1.0 => ./redisproto/
