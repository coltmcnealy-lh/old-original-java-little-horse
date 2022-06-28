#!/bin/bash

SCRIPT_DIR=$( cd -- "$( dirname -- "${BASH_SOURCE[0]}" )" &> /dev/null && pwd )

protoc --java_out=${SCRIPT_DIR}/app/src/main/java/ -I=/home/coltmcnealy/go/src/littlehorse.io/littlehorse/lh_proto/ ~/go/src/littlehorse.io/littlehorse/lh_proto/lh_proto.proto --experimental_allow_proto3_optional
