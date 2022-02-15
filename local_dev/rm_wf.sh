#!/bin/bash

kubectl delete deploy -l littlehorse.io/wfSpecName=$1
kubectl delete svc -l littlehorse.io/wfSpecName=$1
