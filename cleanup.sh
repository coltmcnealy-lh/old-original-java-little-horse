#!/bin/bash

kubectl delete svc -l littlehorse.io/active="true" --all-namespaces
kubectl delete deploy -l littlehorse.io/active="true" --all-namespaces
