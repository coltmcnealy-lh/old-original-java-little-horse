import json
from pprint import pprint as pp
import sys
import time
import requests


URL = "http://localhost:5000"

TYPES = {
    "eev": "ExternalEventDef",
    "task": "TaskDef",
    "wf": "WFSpec"
}

def add_thing(filename):
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    filename = filename.split('.')[-2]
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


if __name__ == '__main__':
    for fname in sys.argv[1:]:
        if not fname.endswith('wf.json'):
            add_thing(fname)
            time.sleep(0.1)

    # Add the workflows only after the tasks have been created so we don't have
    # phantom dependencies
    for fname in sys.argv[1:]:
        if fname.endswith('wf.json'):
            add_thing(fname)
            time.sleep(0.1)