from pprint import pprint as pp
import json
import requests
import time


URL = "http://localhost:30000"


def print_prettily(response):
    strdata = response.content.decode()
    try:
        pp(json.loads(strdata))
    except Exception as exn:
        print(strdata)


task_definition = {
    "name": "task1",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/sleep.py", "<<someVar>>"],
}

external_event_definition = {
    "name": "demo-eev"
}


wf_definition = {
    "name": "my-wf",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "interruptThread": {
            "nodes": {
                "first": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "someVar": {
                            "wfRunVariableName": "name"
                        }
                    },
                    "variableMutations": {
                        "name": {
                            "operation": "SET",
                            "literalValue": "setByInterrupt"
                        }
                    }
                },
            },
            "edges":[]
        },
        "entrypointThread": {
            "variableDefs": {
                "name": {
                    "type": "STRING",
                    "defaultValue": "Initial value of the var",
                }
            },
            "interruptDefs": {
                "demo-eev": {
                    "threadSpecName": "interruptThread"
                },
            },
            "nodes": {
                "a": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "someVar": {
                            "wfRunVariableName": "name"
                        }
                    }
                },
                "b": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "someVar": {
                            "wfRunVariableName": "name"
                        }
                    }
                },
            },
            "edges": [{
                "sourceNodeName": "a",
                "sinkNodeName": "b"
            },
            ]
        }
    }
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition)
print_prettily(create_task_def_response)

create_eev_def_response = requests.post(f"{URL}/externalEventDef", json=external_event_definition)
print_prettily(create_eev_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

