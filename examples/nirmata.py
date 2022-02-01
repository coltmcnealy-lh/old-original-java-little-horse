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


big_req = {
    "variables": {
        "name": "some name"
    },
    'wfSpec':  {
        "name": "my-wf",
        "entrypointThreadName": "entrypointThread",
        "threadSpecs": {
            "entrypointThread": {
                "variableDefs": {
                    "name": {
                        "type": "STRING"
                    },
                },
                "entrypointNodeName": "firstNode",
                "nodes": {
                    "firstNode": {
                        "nodeType": "TASK",
                        "taskDef": {
                            "taskQueueName": "task1",
                            "taskType": "my-favorite-type",
                            "requiredVars": {
                                "theName": {
                                    "type": "STRING",
                                    "defaultValue": "asdf"
                                },
                            },
                        },
                        "variables": {
                            "theName": {
                                "wfRunVariableName": "name"
                            }
                        },
                    },
                },
                "edges": []
            }
        }
    }
}

run_wf_response = requests.post(f"{URL}/wfRun", json=big_req)
print_prettily(run_wf_response)
