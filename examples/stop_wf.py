import sys
import json
import requests


wf_run_id = sys.argv[1]
URL = "http://localhost:5000"

response = requests.post(f"{URL}/WFRun/stop/{wf_run_id}/0")
response.raise_for_status()

print(f"Successfully stoped WFRun {wf_run_id}")
