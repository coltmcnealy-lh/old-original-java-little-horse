import sys
import json
import requests


key = sys.argv[1]
val = sys.argv[2]
URL = "http://localhost:5000"

response = requests.get(f"{URL}/WFRunAlias/{key}/{val}")
response.raise_for_status()

out = response.json()['result']

if out is not None:
    out = out['entries']
    result = [thing['objectId'] for thing in out]
else:
    result = []

print(json.dumps(result))