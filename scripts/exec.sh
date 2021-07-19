#!/bin/sh
PORT=$2
if [ -z $PORT ]; then
    PORT=6379
fi
printf $1"\r\n" | nc localhost $PORT
