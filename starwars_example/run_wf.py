import json
import requests
import sys
from pprint import pprint as pp
import os


URL = "http://localhost:5000"

wf_run_schema = {
    "wfSpecId": "starwars"
}

run_wf_response = requests.post(f'{URL}/WFRun', json=wf_run_schema)
run_wf_response.raise_for_status()

out = run_wf_response.json()
if out['status'] != "OK":
    os.system(f"echo '{run_wf_response.content.decode()}' | jq .")
    raise RuntimeError("had an error")

wf_run_id = out['objectId']
print(wf_run_id)
