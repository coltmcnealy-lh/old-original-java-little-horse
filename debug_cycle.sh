#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

kubectl delete deploy -llittlehorse.io/wfSpecId
kubectl delete svc -llittlehorse.io/wfSpecId

kubectl scale deploy little-horse-api --replicas=0
${SCRIPT_DIR}/local_dev/reset.sh &
${SCRIPT_DIR}/local_dev/docker_build.sh
kubectl scale deploy little-horse-api --replicas=1

sleep 5
kubectl get po

sleep 3

kubectl logs -f -lapp=little-horse-api
