#!/usr/bin/env bash
set -e

# this works as long as the executed script is not a symlink
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd ${DIR}

function usage {
    echo "$0 {env name: dev/demo/prod} [{rest passed to build.sh}...]"
}

if [[ -z "$1" ]]; then
    usage
    exit 1
fi

UBIRCH_ENV="$1"
shift

./build.sh ${UBIRCH_ENV} $@ ${EXTRA_BUILD_PARAMS}
./deploy.sh ${UBIRCH_ENV} ${EXTRA_DEPLOY_PARAMS}
