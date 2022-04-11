#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

${SCRIPT_DIR}/../build.sh

docker build -f ${SCRIPT_DIR}/Dockerfile -t little-horse-test ${SCRIPT_DIR}/..
