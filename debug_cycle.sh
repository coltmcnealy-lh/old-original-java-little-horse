#!/bin/bash
set -ex

kubectl scale deploy little-horse-api --replicas=0
./clear_local_data.sh
./docker_build.sh
kubectl scale deploy little-horse-api --replicas=1

sleep 5
kubectl get po

sleep 3

kubectl logs -f -lapp=little-horse-api
