import argparse
import json
import time
import requests

import os

from lh_harness.test_case_schema import TestSuite
from lh_harness.utils import (
    get_task_def,
    cleanup_case_name,
    get_root_dir
)
from lh_sdk.utils import (
    get_taskdefs_for_wf,
    get_external_events_for_wf,
    add_resource,
)


DEFAULT_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")



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
        "cases", nargs='?', default=[],
        help="Names of test cases to run. If left blank, will deploy all cases."
    )

    ns = parser.parse_args()

    if ns.cases is None or len(ns.cases) == 0:
        cases = os.listdir(os.path.join(
            get_root_dir(), 'tests')
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
        with open(f'tests/external_events/{eev}.json', 'r') as f:
            data = json.loads(f.read())
        add_resource(
            "ExternalEventDef", data, ns.api_url
        )

    time.sleep(0.5)

    for wf in all_wfs:
        response = requests.post(f"{ns.api_url}/WFSpec", json=wf)
        try:
            response.raise_for_status()
        except Exception as exn:
            print(response.content.decode())
            raise exn

        wf_id = response.json()['objectId']
        print(f"Successfully created WFSpec {wf['name']}: {wf_id}")
