SRC='main.go util.go server.go cache.go commands.go range.go metrics.go replication.go geo.go preparer.go runner.go compact.go queue.go config.go'

MONTH=$(date -u +%m)
if [[ "$MONTH" == "10" ]]; then
    MONTH=A
elif [[ "$MONTH" == "11" ]]; then
    MONTH=B
elif [[ "$MONTH" == "12" ]]; then
    MONTH=C
else
    MONTH=$(echo $MONTH | cut -c 2-2)
fi

COMMIT=$(git log --pretty=format:'%h' -n 1)
VERSION=$(($(date -u +%y)-20))
VERSION=${VERSION}.${MONTH}$(date -u +%d).$(($(date +%s) % 86400 / 960 + 10)).$COMMIT
echo 'building' $VERSION

go build -ldflags "-X main.Version=$VERSION" -o s2db $SRC
mkdir -p slave_dir
cp s2db slave_dir/

if [[ "$1" == "linux" ]]; then
    env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o s2db $SRC 
fi
