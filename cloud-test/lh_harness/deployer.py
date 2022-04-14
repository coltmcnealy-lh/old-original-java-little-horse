import argparse
import json
import time
import requests

import os

from lh_harness.check_models import TestSuite
from lh_harness.sdk import get_task_def
from lh_harness.utils import cleanup_case_name, get_file_location


DEFAULT_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")


def add_thing(filename, type_name, api_url):
    with open(filename, 'r') as f:
        data = json.loads(f.read())

    response = requests.post(f"{api_url}/{type_name}", json=data)
    try:
        response.raise_for_status()
    except Exception as exn:
        print(response.content.decode())
        raise exn

    j = response.json()
    if j['status'] != 'OK':
        print(json.dumps(response.json()))
    else:
        print(f"Successfully created {type_name} {j['result']['objectId']}")


def iter_all_nodes(wf_spec: dict):
    threads = [wf_spec['threadSpecs'][k] for k in wf_spec['threadSpecs'].keys()]

    for thread in threads:
        for node_name in thread['nodes'].keys():
            yield thread['nodes'][node_name]


def get_taskdefs_for_wf(wf_spec: dict):
    task_defs = set({})

    for node in iter_all_nodes(wf_spec):
        if node['nodeType'] == 'TASK':
            task_defs.add(node['taskDefName'])
    
    return task_defs


def get_external_events_for_wf(wf_spec: dict):
    eevs = set({})

    for node in iter_all_nodes(wf_spec):
        if node['nodeType'] == 'EXTERNAL_EVENT':
            eevs.add(node['externalEventDefName'])

    threads = [wf_spec['threadSpecs'][k] for k in wf_spec['threadSpecs'].keys()]

    for thread in threads:
        if thread.get('interruptDefs') is None:
            continue

        idefs = thread['interruptDefs']
        for eev_name in idefs.keys():
            eevs.add(eev_name)

    return eevs


def get_specs_for_testcase(test_filename):
    with open(test_filename, 'r') as f:
        data = json.loads(f.read())

    test_suite = TestSuite(**data)
    wf = test_suite.wf_spec

    return get_taskdefs_for_wf(wf), get_external_events_for_wf(wf), wf


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Deploy a test case")

    parser.add_argument(
        "--api-url", '-u', action='store', default=DEFAULT_URL,
        help=f"URL for LittleHorseAPI. Default: {DEFAULT_URL}"
    )
    parser.add_argument(
        "--cases", '-c', nargs='+', default=[],
        help="Names of test cases to run. If left blank, will deploy all cases."
    )

    ns = parser.parse_args()

    if ns.cases is None or len(ns.cases) == 0:
        cases = os.listdir(os.path.join(
            get_file_location(), '..', 'tests/test_cases')
        )
    else:
        cases = ns.cases

    cases = [cleanup_case_name(case) for case in cases]

    all_tasks = set({})
    all_eevs = set({})
    all_wfs = []

    for case in cases:
        new_tasks, new_eevs, wf = get_specs_for_testcase(case)
        all_tasks.update(new_tasks)
        all_eevs.update(new_eevs)
        all_wfs.append(wf)

    for td_name in all_tasks:
        task_def = get_task_def(td_name)
        response = requests.post(f"{ns.api_url}/TaskDef", json=task_def)
        try:
            response.raise_for_status()
        except Exception as exn:
            print(response.content.decode())
            raise exn
        print(f"Successfully created TaskDef {td_name}")

    for eev in all_eevs:
        add_thing(
            f'tests/external_events/{eev}.json', "ExternalEventDef", ns.api_url
        )

    time.sleep(0.5)

    for wf in all_wfs:
        response = requests.post(f"{ns.api_url}/WFSpec", json=wf)
        response.raise_for_status()
        wf_id = response.json()['objectId']
        print(f"Successfully created WFSpec {wf['name']}: {wf_id}")
