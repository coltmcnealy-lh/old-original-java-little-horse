#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

kubectl scale deploy little-horse-api --replicas=0 &
kubectl delete deploy -llittlehorse.io/wfSpecId &
kubectl delete svc -llittlehorse.io/wfSpecId &

# ${SCRIPT_DIR}/local_dev/reset.sh &

${SCRIPT_DIR}/local_dev/docker_build.sh &
docker-compose -f ${SCRIPT_DIR}/local_dev/docker-compose.yml down && docker-compose -f ${SCRIPT_DIR}/local_dev/docker-compose.yml up -d
wait

kubectl scale deploy little-horse-api --replicas=1

sleep 5
kubectl get po

sleep 3

kubectl logs -f -lapp=little-horse-api
