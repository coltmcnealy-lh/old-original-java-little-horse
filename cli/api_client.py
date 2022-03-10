import argparse
import requests
import json
import os


DEFAULT_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000/")
if not DEFAULT_URL.endswith("/"):
    DEFAULT_URL += "/"


class Client:
    def __init__(self, url=DEFAULT_URL):
        self.url = url

    def get(self, resource_name, resource_id):
        url = f'{self.url}{resource_name}/{resource_id}'
        response = requests.get(url)
        response.raise_for_status()
        if response.json()['objectId'] != None:
            return response.json()
        
        url = f'{self.url}{resource_name}Alias/name/{resource_id}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def get_all(self, resource_name):
        url = f'{self.url}{resource_name}/{resource_id}'
        response = requests.get(url)
        response.raise_for_status()
        if response.json()['objectId'] != None:
            return response.json()
        
        url = f'{self.url}{resource_name}Alias/name/{resource_id}'
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    def delete(self, resource_name, resource_id):
        pass

    def add(self, resource_name, resource_spec):
        pass

    def post(self, path, payload):
        response = requests.post(f"{self.url}{path}", json=payload)
        response.raise_for_status()
        return response.json()

    def delete(self, path, payload):
        response = requests.delete(f"{self.url}{path}", json=payload)
        response.raise_for_status()
        return response.json()

    def get(self, path):
        response = requests.get(f"{self.url}{path}")
        response.raise_for_status()
        return response.json()

    def post_task(self, task_def: dict):
        response = requests.post(f"{DEFAULT_URL}TaskDef", json=task_def)
        response.raise_for_status()
        return response.json()

    def post_wf_spec(self, wf_spec: dict):
        response = requests.post(f"{DEFAULT_URL}WFSpec", json=wf_spec)
        response.raise_for_status()
        return response.json()

    def get_wf_spec(self, wf_spec_id: str):
        response = requests.get(f"{DEFAULT_URL}WFSpec/{wf_spec_id}")
        response.raise_for_status()

        if response.json()['objectId'] == None:
            response = requests.get(f"{DEFAULT_URL}WFSpecAlias/name/{wf_spec_id}")
            response.raise_for_status()

        return response.json()

    def get_task_def(self, task_def_id: str):
        response = requests.get(f"{DEFAULT_URL}TakDef/{task_def_id}")
        response.raise_for_status()

        if response.json()['objectId'] == None:
            response = requests.get(f"{DEFAULT_URL}TaskDefAlias/name/{task_def_id}")
            response.raise_for_status()

        return response.json()

    def get_wf_run(self, wf_run_id: str):
        response = requests.get(f"{DEFAULT_URL}WFRun/{wf_run_id}")
        response.raise_for_status()
        return response.json()

    def post_wf_run(self, wf_spec_id: str, variables: dict = None):
        response = requests.post(f"{DEFAULT_URL}WFRun", json={
            "variables": variables,
            "wfSpecId": wf_spec_id,
        })
        response.raise_for_status()
        return response.json()