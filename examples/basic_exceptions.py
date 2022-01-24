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
    "name": "unreliable-task",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/task_unreliable.py"],
}

task_definition1 = {
    "name": "task",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/task1.py", "foo"],
}

wf_definition = {
    "name": "my-wf",
    "entrypointThreadName": "entrypointThread",
    "threadSpecs": {
        "entrypointThread": {
            "entrypointNodeName": "firstNode",
            "nodes": {
                "firstNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "unreliable-task",
                    "baseExceptionhandler": {
                        "shouldResume": True,
                        "handlerThreadSpecName": "handlerThread",
                    }
                },
                "secondNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task",
                },
            },
            "edges": [{
                "sourceNodeName": "firstNode",
                "sinkNodeName": "secondNode"
            },
            ]
        },
        "handlerThread":{
            "nodes": {
                "myNode": {
                    "nodeType": "TASK",
                    "taskDefinitionName": "task",
                }
            }
        }
    }
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition1)
print_prettily(create_task_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

