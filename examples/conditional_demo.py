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
    "name": "task3",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/task3.py", "<<personName>>"],
}


wf_definition = {
    "name": "my-wf",
    "nodes": {
        "firstNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "task1",
            "variables": {
                "personName": {
                    "wfRunVariableName": "name"
                }
            }
        },
        "secondNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "task2",
            "variables": {
                "personNameSecondTask": {
                    "nodeName": "firstNode",
                    "jsonPath": "$.stdout.person"
                }
            }
        },
        "thirdNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "task3",
            "variables": {
                "personName": {
                    "nodeName": "firstNode",
                    "jsonPath": "$.stdout.person"
                }
            }
        }
    },
    "edges": [{
        "sourceNodeName": "firstNode",
        "sinkNodeName": "secondNode",
        "condition": {
            "leftSide": {
                "nodeName": "firstNode",
                "jsonPath": "$.stdout.person"
            },
            "rightSide": {
                "literalValue": "colt___"
            },
            "comparator": "EQUALS"
        }
    }, {
        "sourceNodeName": "firstNode",
        "sinkNodeName": "thirdNode",
    }]
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition1)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition2)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition3)
print_prettily(create_task_def_response)

time.sleep(2)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)
