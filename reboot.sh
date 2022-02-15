#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

kubectl delete po -l littlehorse.io/active="true" --all-namespaces --wait=false

rm -r /tmp/kafkaState
# kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
# kubectl delete ns kafka --wait=false

cd $SCRIPT_DIR/local_dev && docker-compose down && docker-compose up -d
