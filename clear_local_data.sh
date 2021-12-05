#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

cd $SCRIPT_DIR/local_dev
docker-compose down && docker-compose up -d

rm -r /tmp/kafkaState
