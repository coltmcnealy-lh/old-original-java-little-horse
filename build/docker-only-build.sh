#!/bin/bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

cd ${SCRIPT_DIR}/..

docker build -f ${SCRIPT_DIR}/Dockerfile.onlyDocker -t little-horse-api .

