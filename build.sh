#!/usr/bin/env bash
set -e

# this works as long as the executed script is not a symlink
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd ${DIR}

function usage {
    echo "$0 {env name: dev/demo/prod} [{rest passed to maven}...]"
}

if [[ -z "$1" ]]; then
    usage
    exit 1
fi

BUILD_NUM="$1"
shift

set -x
mvn deploy -Dbuild.number=${BUILD_NUM} $@
