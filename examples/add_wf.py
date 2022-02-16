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

task_queue = {
    "name": "my-task-queue",
    "partitions": 3,
}

task_definition = {
    "name": "task1",
    "taskQueueName": "my-task-queue",
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
                    "taskDefName": "task1",
                }
            },
            "edges": [{
                "sourceNodeName": "firstNode",
                "sinkNodeName": "secondNode"
            }]
        }
    }
}


create_tq_response = requests.post(f"{URL}/TaskQueue", json=task_queue)
print_prettily(create_tq_response)
create_task_def_response = requests.post(f"{URL}/TaskDef", json=task_definition)
print_prettily(create_task_def_response)
create_wf_response = requests.post(f"{URL}/WFSpec", json=wf_definition)
print(create_wf_response.json())

