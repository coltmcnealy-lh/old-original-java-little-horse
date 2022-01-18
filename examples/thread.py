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


task_definition3 = {
    "name": "slowtask",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python", "examples/sleep.py"]
}

wf_definition = {
    "name": "my-wf",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "subthread":{
            "variableDefs": {
                "name2": {
                    "type": "STRING",
                    "defaultValue": "default val for name2 str thingy"
                }
            },
            "nodes": {
                "first": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "slowtask",
                },
                "second": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task1",
                    "variables": {
                        "personName": {
                            "wfRunVariableName": "name2"
                        }
                    }
                }
            },
            "edges":[{
                "sourceNodeName": "first",
                "sinkNodeName": "second",
            }]
        },
        "entrypointThread": {
            "variableDefs": {
                "name": {
                    "type": "STRING"
                },
                "secondName": {
                    "type": "STRING",
                }
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
                    "nodeType": "SPAWN_THREAD",
                    "threadSpawnThreadSpecName": "subthread",
                },
                "thirdNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task2",
                    "variables": {
                        "personNameSecondTask": {
                            "wfRunVariableName": "secondName",
                        }
                    },
                },
                "fourthNode": {
                    "nodeType": "WAIT_FOR_THREAD",
                    "threadWaitSourceNodeName": "secondNode",
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
            {
                "sourceNodeName": "thirdNode",
                "sinkNodeName": "fourthNode"
            },
            ]
        }
    }
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition1)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition2)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition3)
print_prettily(create_task_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

