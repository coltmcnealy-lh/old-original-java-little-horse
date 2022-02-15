#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

kubectl delete deploy -llittlehorse.io/wfSpecGuid
kubectl delete svc -llittlehorse.io/wfSpecGuid

kubectl scale deploy little-horse-api --replicas=0
${SCRIPT_DIR}/reset.sh &
${SCRIPT_DIR}/docker_build.sh
kubectl scale deploy little-horse-api --replicas=1

sleep 5
kubectl get po

sleep 3

kubectl logs -f -lapp=little-horse-api
