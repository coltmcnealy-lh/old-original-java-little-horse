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


task_definition1 = {
    "name": "task1",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/task1.py", "<<personName>>"],
}


task_definition2 = {
    "name": "task2",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/task2.py", "<<personNameSecondTask>>"],
}


wf_definition = {
    "name": "my-wf",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "entrypointThread": {
            "variableDefs": {
                "name": {
                    "type": "STRING"
                },
                "secondName": {
                    "type": "STRING",
                },
            },
            "entrypointNodeName": "firstNode",
            "nodes": {
                "firstNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "personName": {
                            "wfRunVariableName": "name"
                        }
                    },
                    "variableMutations": {
                        "secondName": {
                            "operation": "SET",
                            "jsonPath": "$.stdout.person"
                        },
                    }
                },
                "secondNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task2",
                    "variables": {
                        "personNameSecondTask": {
                            "wfRunVariableName": "secondName",
                        }
                    },
                }
            },
            "edges": [{
                "sourceNodeName": "firstNode",
                "sinkNodeName": "secondNode"
            },
            # {
            #     "sourceNodeName": "secondNode",
            #     "sinkNodeName": "firstNode",
            #     "condition": {
            #         "leftSide": {
            #             "wfRunVariableName": "counter"
            #         },
            #         "rightSide": {
            #             "literalValue": 500
            #         },
            #         "comparator": "LESS_THAN",
            #     }
            # }
            ]
        }
    }
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition1)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition2)
print_prettily(create_task_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

