module github.com/coyove/s2db

go 1.16

require (
	github.com/coyove/common v0.0.0-20210601082815-1e7f2ca0bb84
	github.com/coyove/script v0.0.0-20211022115741-03647de15fe9
	github.com/go-redis/redis/v8 v8.11.0
	github.com/mmcloughlin/geohash v0.10.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/tidwall/gjson v1.10.1 // indirect
	go.etcd.io/bbolt v1.3.6
	gopkg.in/natefinch/lumberjack.v2 v2.0.0

)

replace go.etcd.io/bbolt v1.3.6 => ./bbolt
