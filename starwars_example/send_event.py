import json
import requests
import sys


wf_run_id = sys.argv[1]
event_name = sys.argv[2]
event_content = sys.argv[3]


URL = "http://localhost:5000"

event_id = requests.get(
    f'{URL}/ExternalEventDefAlias/name/{event_name}'
).json()['objectId']


requests.post(
    f'{URL}/externalEvent/{event_id}/{wf_run_id}',
    data=event_content,
)
