from confluent_kafka import Producer # type: ignore

from datetime import datetime as dt
import json
import time

import sys

from executor.executor_config import KAFKA_BOOTSTRAP_SERVERS

wf_spec_id = sys.argv[1]

prod = Producer(**{
    "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
    "client.id": "foo",
    'partitioner': 'murmur2',
})

def send_event(wf_run_id):
    request = {
        "wfSpecId": wf_spec_id, 
        "wfRunId": wf_run_id
    }

    event = {
        "wfSpecId": wf_spec_id,
        "wfSpecName": "test_wf",
        "wfRunId": wf_run_id,
        "type": "WF_RUN_STARTED",
        "timestamp": int(time.time() * 1000),
        "content": json.dumps(request)
    }

    prod.produce(
        "wfEvents__test_wf-" + wf_spec_id[:wf_spec_id.find("-")],
        json.dumps(event).encode(), key=wf_run_id
    )


for i in range(1000):
    send_event("req" + str(i))
    if i % 100 == 0:
        prod.poll(0)
        print(i, end = ' ', flush=True)


prod.flush()


print("Done!!!")