#!/usr/bin/env bash
set -e

# this works as long as the executed script is not a symlink
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd ${DIR}

mvn deploy -Dbuild.number=${BUILD_NUM:-devbuild} $@
