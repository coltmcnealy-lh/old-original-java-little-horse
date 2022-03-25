#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

kubectl scale statefulset little-horse-api --replicas=0 --namespace default
kubectl delete po --namespace default --selector app=little-horse-api # speed it up
kubectl delete deploy --selector io.littlehorse/deployedBy

kubectl exec -nkafka lh-kafka-kafka-0 -it -- bash -c '
export TOPICS=$(bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)
if test $TOPICS
then
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $(echo $TOPICS | tr "\n" "|")
fi
'

./build.sh
kind load docker-image --name littlehorse little-horse-api

kubectl scale statefulset little-horse-api --replicas=2 --namespace default


sleep 8
kubectl get po
sleep 5

${SCRIPT_DIR}/enterprise/kind/lhport &

kubectl logs -f -lapp=little-horse-api --namespace default
