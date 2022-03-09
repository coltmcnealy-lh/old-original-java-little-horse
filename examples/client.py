import requests
import json
import os


URL = os.getenv("LHORSE_API_URL", "http://localhost:5000/")
if not URL.endswith("/"):
    URL += "/"


def post_task(task_def: dict, url=URL):
    response = requests.post(f"{URL}TaskDef", json=task_def)
    response.raise_for_status()
    return response.json()


def post_wf_spec(wf_spec: dict, url=URL):
    response = requests.post(f"{URL}WFSpec", json=wf_spec)
    response.raise_for_status()
    return response.json()


def get_wf_spec(wf_spec_id: str, url=URL):
    response = requests.get(f"{URL}WFSpec/{wf_spec_id}")
    response.raise_for_status()

    if response.json()['objectId'] == None:
        response = requests.get(f"{URL}WFSpecAlias/name/{wf_spec_id}")
        response.raise_for_status()
    
    return response.json()


def get_task_def(task_def_id: str, url=URL):
    response = requests.get(f"{URL}TakDef/{task_def_id}")
    response.raise_for_status()

    if response.json()['objectId'] == None:
        response = requests.get(f"{URL}TaskDefAlias/name/{task_def_id}")
        response.raise_for_status()

    return response.json()


def get_wf_run(wf_run_id: str, url=URL):
    response = requests.get(f"{URL}WFRun/{wf_run_id}")
    response.raise_for_status()
    return response.json()


def post_wf_run(wf_spec_id: str, url=URL, variables: dict = None):
    response = requests.post(f"{URL}WFRun", json={
        "variables": variables,
        "wfSpecId": wf_spec_id,
    })
    response.raise_for_status()
    return response.json()
