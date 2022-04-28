#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

set -ex

javac -cp ${SCRIPT_DIR}/../../../app/build/libs/app-all.jar:. MyTask.java

docker build -f ${SCRIPT_DIR}/Dockerfile.myjavatask -t lh-myjavatask:latest ${SCRIPT_DIR}
