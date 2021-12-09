#!/bin/bash

kubectl delete deploy -l littlehorse.io/wfSpecGuid=$1
kubectl delete svc -l littlehorse.io/wfSpecGuid=$1
