from pprint import pprint as pp
import json
import requests
import time

from lib import form_task


URL = "http://localhost:5000"


def print_prettily(response):
    strdata = response.content.decode()
    try:
        pp(json.loads(strdata))
    except Exception as exn:
        print(strdata)


walk_into_room_task = form_task(
    "walk_into_room",
    ['/starwarstasks/walk_into_room.py']
)

greet_person_task = form_task(
    "greet_person",
    ['/starwarstasks/greeting.py', '<<person_type>>'],
    required_vars={"person_type": "INT"}
)

mystery_person_event = {
    "name": "mystery_person_arrives"
}


wf_definition = {
    "name": "starwars",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "entrypointThread": {
            "entrypointNodeName": "walk_into_room",
            "variableDefs": {
                "mystery_person_type": {
                    "type": "STRING",
                },
                "greeting": {
                    "type": "STRING",
                }
            },
            "nodes": {
                "walk_into_room": {
                    "nodeType": "TASK",
                    "taskDefName": "walk_into_room",
                },
                "wait_for_mystery_person": {
                    "nodeType": "EXTERNAL_EVENT",
                    "externalEventDefName": "mystery_person_arrives",
                    "variableMutations": {
                        "mystery_person_type": {
                            "copyDirectlyFromNodeOutput": True,
                            "operation": "ASSIGN",
                        }
                    }
                },
                "greet_force_user": {
                    "nodeType": "TASK",
                    "taskDefName": "greet_person",
                    "variableMutations": {
                        "greeting": {
                            "operation": "ASSIGN",
                            "copyDirectlyFromNodeOutput": True
                        }
                    },
                    "variables": {
                        "person_type": {
                            "wfRunVariableName": "mystery_person_type",
                        }
                    }
                }
            },
            "edges": [{
                "sourceNodeName": "walk_into_room",
                "sinkNodeName": "wait_for_mystery_person"
            }, {
                "sourceNodeName": "wait_for_mystery_person",
                "sinkNodeName": "greet_force_user",
                "condition": {
                    "leftSide": {
                        "wfRunVariableName": "mystery_person_type",
                    },
                    "comparator": "IN",
                    "rightSide": {
                        "literalValue": ["jedi", "sith"],
                    }
                }
            }]
        }
    }
}


response = None

try:
    response = requests.post(f"{URL}/TaskDef", json=greet_person_task)
    response.raise_for_status()
    print("Created greet_person TaskDef")

    response = requests.post(f"{URL}/TaskDef", json=walk_into_room_task)
    response.raise_for_status()
    print("Created walk_into_room TaskDef")

    response = requests.post(
        f"{URL}/ExternalEventDef", json=mystery_person_event
    )
    response.raise_for_status()
    print("created mystery_person_arrives ExternalEventDef")

    time.sleep(1)
    response = requests.post(f"{URL}/WFSpec", json=wf_definition)
    response.raise_for_status()
    print("Created starwars WFSpec")

except Exception as exn:
    print(response.content.decode())
    raise exn
