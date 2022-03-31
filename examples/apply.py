import json
from pprint import pprint as pp
import sys
import requests


URL = "http://localhost:5000"

TYPES = {
    "eev": "ExternalEventDef",
    "task": "TaskDef",
    "wf": "WFSpec"
}

if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        data = json.loads(f.read())

    filename = sys.argv[1].split('.')[-2]
    type_name = None
    for t in TYPES.keys():
        if filename.endswith(t):
            type_name = TYPES[t]
    if type_name is None:
        raise RuntimeError(
            "Invalid file extension. Must be in " + str(list(TYPES.keys()))
        )

    response = requests.post(f"{URL}/{type_name}", json=data)
    print(response.json())
