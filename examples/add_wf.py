from pprint import pprint as pp
import json
import requests
import time


URL = "http://localhost:5000"


def print_prettily(response):
    strdata = response.content.decode()
    try:
        pp(json.loads(strdata))
    except Exception as exn:
        print(strdata)


task_definition1 = {
    "name": "task1",
    "deployMetadata": json.dumps({
        "dockerImage": "little-horse-api:latest",
        "metadata": json.dumps({
            "bashCommand": ["ls"]
        }),
        "secondaryValidatorClassName": "little.horse.examples.bashExecutor.BashValidator",
        "taskExecutorClassName": "little.horse.examples.bashExecutor.BashExecutor",
    })
}

task_definition2 = {
    "name": "task2",
    "deployMetadata": json.dumps({
        "dockerImage": "little-horse-api:latest",
        "metadata": json.dumps({
            "bashCommand": ["ls -a"]
        }),
        "secondaryValidatorClassName": "little.horse.examples.bashExecutor.BashValidator",
        "taskExecutorClassName": "little.horse.examples.bashExecutor.BashExecutor",
    })
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
                    "taskDefName": "task1",
                },
                "secondNode": {
                    "nodeType": "TASK",
                    "taskDefName": "task2",
                }
            },
            "edges": [{
                "sourceNodeName": "firstNode",
                "sinkNodeName": "secondNode"
            }]
        }
    }
}

create_task_def_response = requests.post(f"{URL}/TaskDef", json=task_definition1)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/TaskDef", json=task_definition2)
print_prettily(create_task_def_response)

time.sleep(0.3)
create_wf_response = requests.post(f"{URL}/WFSpec", json=wf_definition)
print(create_wf_response.json())

