#!/bin/bash

set -ex

kubectl -nkafka exec lhorse-kafka-0 -- bash -c '#!/bin/bash

TOPICS=$(bin/kafka-topics.sh --bootstrap-server host.docker.internal:32100 --list )

for T in $TOPICS
do
  if [ "$T" != "__consumer_offsets" ]; then
    if  [ "$T" != "__strimzi-topic-operator-kstreams-topic-store-changelog" ]; then
      if [ "$T" != "__strimzi_store_topic" ]; then
        bin/kafka-topics.sh --bootstrap-server host.docker.internal:32100 --delete --topic $T &
  	echo "deleting topic $T"
      fi
    fi
  fi
done'

