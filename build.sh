#!/bin/bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

gradle fatJar

docker build -f Dockerfile -t little-horse-api $SCRIPT_DIR
