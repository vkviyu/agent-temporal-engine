#!/bin/sh
set -eu
binary=$1
shift
package=${PWD#/Users/zhaozhiqiang/work/agent-temporal-engine}
name=$(basename "$(dirname "$binary")")-$(basename "$binary")
docker --context colima cp "$binary" "ate-v1-native-check:/ate-data/bin/$name" >/dev/null
docker --context colima exec -w "/workspace$package" -e TMPDIR=/ate-data/tmp ate-v1-native-check "/ate-data/bin/$name" "$@"
