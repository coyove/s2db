module github.com/coyove/s2db

go 1.16

require (
	github.com/RoaringBitmap/roaring v0.9.4 // indirect
	github.com/coyove/common v0.0.0-20210601082815-1e7f2ca0bb84
	github.com/coyove/nj v0.0.0-20211221042124-1d845727358d
	github.com/coyove/script v0.0.0-20211117063405-15b16c4d0ba7
	github.com/go-redis/redis/v8 v8.11.0
	github.com/kavu/go_reuseport v1.5.0 // indirect
	github.com/mmcloughlin/geohash v0.10.0
	github.com/sirupsen/logrus v1.8.1
	github.com/stretchr/testify v1.6.1 // indirect
	github.com/tidwall/gjson v1.12.1 // indirect
	go.etcd.io/bbolt v1.3.6
	golang.org/x/net v0.0.0-20210405180319-a5a99cb37ef4 // indirect
	gopkg.in/natefinch/lumberjack.v2 v2.0.0

)

replace go.etcd.io/bbolt v1.3.6 => ./bbolt
