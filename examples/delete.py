import json
import sys
import time
from unicodedata import name
import requests


URL = "http://localhost:5000"

TYPES = {
    "eev": "ExternalEventDef",
    "task": "TaskDef",
    "wf": "WFSpec"
}

def delete_thing(filename):
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

    get_id_response = requests.get(f"{URL}/{type_name}Alias/name/{data['name']}")
    get_id_response.raise_for_status()

    get_id_json = get_id_response.json()
    if get_id_json['result'] is None:
        raise RuntimeError("Could not find object with name " + data['name'])
    
    object_id = get_id_json['result']['entries'][0]['objectId']

    response = requests.delete(f"{URL}/{type_name}/{object_id}")
    print(response.json())


if __name__ == '__main__':
    for fname in sys.argv[1:]:
        if not fname.endswith('wf.json'):
            delete_thing(fname)
            time.sleep(0.1)

    # Add the workflows only after the tasks have been created so we don't have
    # phantom dependencies
    for fname in sys.argv[1:]:
        if fname.endswith('wf.json'):
            delete_thing(fname)
            time.sleep(0.1)