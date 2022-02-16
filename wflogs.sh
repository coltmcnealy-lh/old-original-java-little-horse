#!/bin/bash

kubectl logs -f -llittlehorse.io/wfSpecName=$1
