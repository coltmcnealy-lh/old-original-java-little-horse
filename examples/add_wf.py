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

task_definition1 = {
    "name": "task1",
    "taskType": "my-type",
    "taskQueueName": "my-task-queue",
}

task_definition2 = {
    "name": "task3",
    "taskType": "my-type-3",
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
                    "taskDefName": "task3",
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
create_task_def_response = requests.post(f"{URL}/TaskDef", json=task_definition1)
print_prettily(create_task_def_response)
create_task_def_response = requests.post(f"{URL}/TaskDef", json=task_definition2)
print_prettily(create_task_def_response)

time.sleep(0.1)
create_wf_response = requests.post(f"{URL}/WFSpec", json=wf_definition)
print(create_wf_response.json())

