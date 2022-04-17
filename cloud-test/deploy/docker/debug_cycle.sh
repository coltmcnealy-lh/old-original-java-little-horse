#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd ${SCRIPT_DIR}

rm -r ../../alembic/versions/*
set -e

../../../docker/debug_cycle.sh

cd ../..

${SCRIPT_DIR}/setup.sh
