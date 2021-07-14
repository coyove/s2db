rm -rf zset.7z
env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o zset main.go util.go server.go cache.go commands.go range.go && 7z a zset.7z zset
go build -o zset_darwin main.go util.go server.go cache.go commands.go range.go
cp zset slave_dir/

if [[ "$1" == "upload" ]]; then
    scp zset.7z root@8.212.30.157:/root
    scp zset.7z root@47.243.64.196:/data/zset/
fi
