from pprint import pprint as pp
import json
import requests
import time


URL = "http://localhost:5000"
# URL = "http://192.168.59.100:30000" #"http://192.168.49.2:30000"


def form_task(name, bash_command, required_vars=None):
    out = {
        "name": name,
        "taskDeployerClassName": "little.horse.lib.deployers.enterprise.kubernetes.K8sTaskDeployer",
        "deployMetadata": json.dumps({
            "dockerImage": "little-horse-api:latest",
            "metadata": json.dumps({
                "bashCommand": ['python3'] + bash_command
            }),
            "secondaryValidatorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashValidator",
            "taskExecutorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashExecutor",
        })
    }

    if required_vars is not None:
        out['requiredVars'] = {}
        for var in required_vars.keys():
            out['requiredVars'][var] = {
                "type": required_vars[var]
            }

    return out


def print_prettily(response):
    strdata = response.content.decode()
    try:
        pp(json.loads(strdata))
    except Exception as exn:
        print(strdata)

walk_into_room_task = form_task(
    "walk_into_room2",
    ['/starwarstasks/walk_into_room.py']
)

greet_person_task = form_task(
    "greet_person2",
    ['/starwarstasks/greeting.py', '<<person_type>>'],
    required_vars={"person_type": "INT"}
)


wf_definition = {
    "name": "starwars",
    "entrypointThreadName": "entrypointThread",
    "wfDeployerClassName": "little.horse.lib.deployers.enterprise.kubernetes.K8sWorkflowDeployer",
    "threadSpecs": {
        "entrypointThread": {
            "entrypointNodeName": "walk_into_room",
            "variableDefs": {
                "mystery_person_type": {
                    "type": "STRING",
                },
                "greeting": {
                    "type": "STRING",
                }
            },
            "nodes": {
                "walk_into_room": {
                    "nodeType": "TASK",
                    "taskDefName": "walk_into_room2",
                },
                "greet_force_user": {
                    "nodeType": "TASK",
                    "taskDefName": "greet_person2",
                    "variableMutations": {
                        "greeting": {
                            "operation": "ASSIGN",
                            "copyDirectlyFromNodeOutput": True
                        }
                    },
                    "variables": {
                        "person_type": {
                            "literalValue": "jedi",
                        }
                    }
                }
            },
            "edges": [{
                "sourceNodeName": "walk_into_room",
                "sinkNodeName": "greet_force_user"
            }]
        }
    }
}


response = None

try:
    response = requests.post(f"{URL}/TaskDef", json=greet_person_task)
    response.raise_for_status()
    print("Created greet_person TaskDef")

    response = requests.post(f"{URL}/TaskDef", json=walk_into_room_task)
    response.raise_for_status()
    print("Created walk_into_room TaskDef")

    time.sleep(1)
    response = requests.post(f"{URL}/WFSpec", json=wf_definition)
    response.raise_for_status()
    print("Created starwars WFSpec")

except Exception as exn:
    print(response.content.decode())
    raise exn
