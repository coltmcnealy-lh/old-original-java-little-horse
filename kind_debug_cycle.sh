#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

kubectl scale statefulset little-horse-api --replicas=0 --namespace default
kubectl delete deploy --selector io.littlehorse/deployedBy
kubectl delete po -nkafka --selector app.kubernetes.io/instance=lh-kafka

./build.sh
kind load docker-image --name littlehorse little-horse-api

kubectl scale statefulset little-horse-api --replicas=3 --namespace default


sleep 5
kubectl get po
sleep 3

${SCRIPT_DIR}/enterprise/kind/lhport &

kubectl logs -f -lapp=little-horse-api --namespace default
