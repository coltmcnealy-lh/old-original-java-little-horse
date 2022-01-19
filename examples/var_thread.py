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

wf_definition = {
    "name": "my-wf",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "subthread":{
            "variableDefs": {},
            "nodes": {
                "first": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "someVar": {
                            "wfRunVariableName": "name"
                        }
                    },
                },
            },
            "edges":[]
        },
        "entrypointThread": {
            "variableDefs": {
                "name": {
                    "type": "STRING"
                }
            },
            "entrypointNodeName": "firstNode",
            "nodes": {
                "firstNode": {
                    "nodeType": "SPAWN_THREAD",
                    "threadSpawnThreadSpecName": "subthread",
                },
                "secondNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "someVar": {
                            "wfRunVariableName": "name"
                        }
                    }
                },
                "thirdNode": {
                    "nodeType": "WAIT_FOR_THREAD",
                    "threadWaitSourceNodeName": "firstNode",
                }
            },
            "edges": [{
                "sourceNodeName": "firstNode",
                "sinkNodeName": "secondNode"
            },
            {
                "sourceNodeName": "secondNode",
                "sinkNodeName": "thirdNode"
            },
            ]
        }
    }
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition)
print_prettily(create_task_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

