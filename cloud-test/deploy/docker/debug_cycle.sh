#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}

rm -r ../../alembic/versions/*
set -e
docker-compose -p docker-test-harness -f docker-compose.pg.yml down
docker-compose -f ../../../docker/docker-compose.yml down

cd ../..

${SCRIPT_DIR}/setup.sh
