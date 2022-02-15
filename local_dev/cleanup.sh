#!/bin/bash

SCRIPT_DIR="$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )"

kubectl delete svc -l littlehorse.io/active="true" --all-namespaces
kubectl delete deploy -l littlehorse.io/active="true" --all-namespaces

# kubectl delete -f 'https://strimzi.io/install/latest?namespace=kafka' -n kafka
# kubectl delete ns kafka --wait=false

cd $SCRIPT_DIR && docker-compose down
