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


external_event_definition = {
    "name": "demo-eev"
}


task_definition = {
    "name": "dummy",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/dummy_task.py"],
}

task_definition2 = {
    "name": "sleep-a-while",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/sleep.py"],
}


signal_wf_definition = {
    "name": "signal-wf",
    "nodes": {
        "firstNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "dummy",
        },
        "secondNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "dummy",
        }
    },
    "edges": [{
        "sourceNodeName": "firstNode",
        "sinkNodeName": "secondNode",
    }]
}


wf_definition = {
    "name": "my-wf",
    "nodes": {
        "firstNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "dummy",
        },
        "secondNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "sleep-a-while"
        },
        "thirdNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "dummy",
        }
    },
    "edges": [{
        "sourceNodeName": "firstNode",
        "sinkNodeName": "secondNode",
    },
    {
        "sourceNodeName": "secondNode",
        "sinkNodeName": "thirdNode",
    }],
    "signalHandlers": [{
        "externalEvent": "demo-eev",
        "wfSpecName": "signal-wf",
    }]
}


requests.post(f"{URL}/taskDef", json=task_definition)
requests.post(f"{URL}/taskDef", json=task_definition2)
requests.post(f"{URL}/externalEventDef", json=external_event_definition)


time.sleep(0.01)
requests.post(f"{URL}/wfSpec", json=signal_wf_definition)
time.sleep(0.01)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)
