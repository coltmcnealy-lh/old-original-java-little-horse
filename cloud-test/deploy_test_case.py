import json
import sys
import time
import requests

import os


URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")

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
    try:
        response.raise_for_status()
    except:
        print(response.content.decode())
        return

    j = response.json()
    if j['status'] != 'OK':
        print(json.dumps(response.json()))
    else:
        print(f"Successfully created {type_name} {j['result']['objectId']}")


def add_test_case(dirname):
    specs = [f for f in os.listdir(dirname + "/specs") if f.endswith('.json')]

    if sum([1 if f.endswith('wf.json') else 0 for f in specs]) != 1:
        raise RuntimeError("Need to provide exactly one WFSpec")

    for f in specs:
        if not f.endswith('wf.json'):
            add_thing(dirname + '/specs/' + f)

    time.sleep(0.2)
    for f in specs:
        if f.endswith('wf.json'):
            add_thing(dirname + '/specs/' + f)


if __name__ == '__main__':
    target = sys.argv[1].lstrip('/')

    directories = os.listdir(target)
    for directory in directories:
        print(directory)
        add_test_case(target + '/' + directory)
