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
    "name": "print-guid",
    "dockerImage": "little-horse-daemon",
    "bashCommand": ["python3", "/examples/printWfRunGuid.py", "<<wfRunGuid>>"],
}


wf_definition = {
    "name": "my-wf",
    "nodes": {
        "firstNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "print-guid",
            "variables": {
                "wfRunGuid": {
                    "wfRunMetadata": "WF_RUN_GUID"
                }
            }
        },
        "secondNode": {
            "nodeType": "EXTERNAL_EVENT",
            "externalEventDefName": "demo-eev",
        },
    },
    "edges": [{
        "sourceNodeName": "firstNode",
        "sinkNodeName": "secondNode",
    }]
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition)
print_prettily(create_task_def_response)

create_eev_def_response = requests.post(f"{URL}/externalEventDef", json=external_event_definition)
print_prettily(create_eev_def_response)


time.sleep(2)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)

