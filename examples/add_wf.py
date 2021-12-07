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


task_definition = {
    "name": "myTaskDefinition",
    "dockerImage": "little-horse",
    "bashCommand": ["python", "test.py"],
}


wf_definition = {
    "name": "my-wf",
    "nodes": {
        "onlyNode": {
            "nodeType": "TASK",
            "taskDefinitionName": "myTaskDefinition",
        }
    },
    "edges": []
}


create_task_def_response = requests.post(f"{URL}/taskDef", json=task_definition)
print_prettily(create_task_def_response)

time.sleep(2)
create_wf_response = requests.post(f"{URL}/wfSpec", json=wf_definition)
print_prettily(create_wf_response)
