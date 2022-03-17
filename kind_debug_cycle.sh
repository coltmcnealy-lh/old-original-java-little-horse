#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

kubectl scale deploy little-horse-api --replicas=0
kubectl delete deploy --selector io.littlehorse/deployedBy

./build.sh
kind load docker-image --name lh little-horse-api

kubectl scale deploy little-horse-api --replicas=1


sleep 5
kubectl get po
sleep 3

lhport &

kubectl logs -f -lapp=little-horse-api
