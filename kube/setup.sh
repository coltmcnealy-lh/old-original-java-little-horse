#!/bin/bash

set -ex

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )
${SCRIPT_DIR}/../build.sh

kind create cluster --name littlehorse --config ${SCRIPT_DIR}/kind-config.yaml

kubectl apply -f ${SCRIPT_DIR}/kafka-ns.yaml
kubectl apply -f ${SCRIPT_DIR}/strimzi-crd.yaml

docker pull quay.io/strimzi/kafka:0.28.0-kafka-3.1.0 &
docker pull quay.io/strimzi/operator:0.28.0 &

wait

kind load docker-image --name littlehorse quay.io/strimzi/kafka:0.28.0-kafka-3.1.0 &
kind load docker-image --name littlehorse quay.io/strimzi/operator:0.28.0 &
kind load docker-image --name littlehorse little-horse-api &

wait

kubectl apply -f ${SCRIPT_DIR}/lh-kafka.yaml

kubectl wait kafka/lh-kafka --for=condition=Ready --timeout=300s -n kafka 


kubectl apply -f ${SCRIPT_DIR}/core-api-0.yaml
sleep 2
kubectl apply -f ${SCRIPT_DIR}/core-api-1.yaml
