#!/bin/bash
set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
cd $SCRIPT_DIR

kubectl scale statefulset little-horse-api --replicas=0 --namespace default &
# Next line is redundant but trust me it speeds it up
kubectl delete po --namespace default --selector app=little-horse-api &
kubectl delete deploy --selector io.littlehorse/deployedBy &

kubectl exec -nkafka lh-kafka-kafka-0 -it -- bash -c '
export TOPICS=$(bin/kafka-topics.sh --bootstrap-server localhost:9092 --list)
if [ ${#TOPICS[@]} -eq 0 ]
then
    echo "Nothing to do!"
else
    bin/kafka-topics.sh --bootstrap-server localhost:9092 --delete --topic $(echo $TOPICS | tr " " "|")
    echo "Deleted topics!!"
fi
' &

../build.sh &

wait

kind load docker-image --name littlehorse little-horse-api

kubectl scale statefulset little-horse-api --replicas=2 --namespace default


sleep 5
kubectl get po
sleep 3

kubectl port-forward svc/little-horse-api 5000:5000 -ndefault &

kubectl logs -f -lapp=little-horse-api --namespace default
