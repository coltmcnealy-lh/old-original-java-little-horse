#!/bin/bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
${SCRIPT_DIR}/../build.sh

docker-compose -f ${SCRIPT_DIR}/docker-compose.yml up -d
