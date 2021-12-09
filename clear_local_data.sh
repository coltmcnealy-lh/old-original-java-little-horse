#!/bin/bash

set -x

kubectl delete deploy --selector littlehorse.io/deployed-by="littlehorse" --all-namespaces
kubectl delete po --selector littlehorse.io/active="true" --all-namespaces
