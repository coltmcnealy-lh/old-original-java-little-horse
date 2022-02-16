import json
import requests
import sys
from pprint import pprint as pp


URL = "http://localhost:30000"


def print_prettily(response):
    strdata = response.content.decode()
    try:
        pp(json.loads(strdata))
    except Exception as exn:
        print(strdata)

wf_spec = requests.get(f"{URL}/WFSpecAlias/name/{sys.argv[1]}")
# print_prettily(wf_spec)

wf_run_schema = {
    "variables": {
        "name": "colt"
    },
    "wfSpecId": wf_spec.json()['objectId']
}

run_wf_response = requests.post(f'{URL}/WFRun', json=wf_run_schema)
print(run_wf_response)
print(run_wf_response.content)
import time
time.sleep(0.3)

response = requests.get(f"{URL}/WFRun/{run_wf_response.json()['guid']}")

print(response.content.decode())
