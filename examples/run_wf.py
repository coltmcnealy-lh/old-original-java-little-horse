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


wf_run_schema = {
    "variables": {
        "name": "colt"
    }
}

run_wf_response = requests.post(f'{URL}/wfRun/{sys.argv[1]}', json=wf_run_schema)
import time
time.sleep(2)

response = requests.get(f"{URL}/wfRun/{run_wf_response.json()['guid']}")

print(response.content.decode())
