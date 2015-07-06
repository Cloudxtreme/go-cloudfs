#!/bin/sh
set -eu
find "${0%/*}" -name '*.proto' -type f | {
  while read f; do
    (
      cd "${f%/*}"
      protoc --go_out=plugins=grpc:. "${f##*/}"
    )
  done
}
