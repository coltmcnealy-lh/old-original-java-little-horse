#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR/..

CONTAINERS=$(docker ps -aq --filter label=io.littlehorse/active)

if [ -z $CONTAINERS ]
then
    echo "No containers to stop."
else
    docker stop $CONTAINERS &
fi

./build.sh


wait

if [ -z $CONTAINERS ]
then
    echo "No containers to remove."
else
    docker rm $CONTAINERS
fi

cd $SCRIPT_DIR
docker-compose up -d

if [ -z $1 ]
then
    echo "Add any random string as an argument to follow logs"
else
    docker logs -f little-horse-api
fi
