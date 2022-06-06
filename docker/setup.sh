#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

docker-compose up -d
sleep 2

psql postgresql://postgres:postgres@localhost:5432/postgres -f ${SCRIPT_DIR}/../sql/000_initial_schema.sql
