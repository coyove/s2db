SRC='main.go server.go server_ex.go zset.go replication.go preparer.go runner.go compact.go config.go metrics.go set.go kv.go'

MONTH=$(date -u +%m)
if [[ "$MONTH" == "10" ]]; then
    MONTH=a
elif [[ "$MONTH" == "11" ]]; then
    MONTH=b
elif [[ "$MONTH" == "12" ]]; then
    MONTH=c
else
    MONTH=$(echo $MONTH | cut -c 2-2)
fi

COMMIT=$(git log --pretty=format:'%h' -n 1)
LOGSHARD=$(grep 'const ShardLogNum = ' server.go | cut -c21-100)
VERSION=$(($(date -u +%y)-20))
VERSION=${VERSION}.${MONTH}$(date -u +%d).$(($(date +%s) % 86400 / 100 + 100))${COMMIT}-${LOGSHARD}
echo 'building' $VERSION

if [[ "$1" == "linux" ]]; then
    env GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o s2db $SRC 
    chmod +x ./s2db
    cp ./s2db slave_dir/
    exit 0
fi

OUT=s2db
if [[ "$1" == "win32" ]]; then
    OUT=s2db.exe
fi

CGO_ENABLED=0 go build -ldflags "-X main.Version=$VERSION" -o $OUT $SRC
mkdir -p slave_dir slave_dir2
cp $OUT slave_dir/
cp $OUT slave_dir2/
