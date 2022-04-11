import json
import os
import time
import sys

import requests

from lh_harness.check_models import Command, TestCase, ThreadRunOutput


URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")


def run_test(wf_spec_id: str, case: TestCase):
    # Step 1: Actually run the workflow.
    wf_run_schema = {
        "wfSpecId": wf_spec_id,
        "variables": case.command.variables,
    }
    run_wf_response = requests.post(f'{URL}/WFRun', json=wf_run_schema)
    run_wf_response.raise_for_status()

    wf_run_id = run_wf_response.json()['objectId']

    # Step 2: Wait for it to complete
    time.sleep(case.timeout)

    # Step 3: see if it actually came out properly
    get_wf_response = requests.get(f"{URL}/WFRun/{wf_run_id}")
    get_wf_response.raise_for_status()

    wf_run = get_wf_response.json()['result']

    if wf_run is None:
        raise RuntimeError("Got null wfrun response!")

    for output in case.output:
        if output.tr_number >= len(wf_run['threadRuns']):
            raise RuntimeError("Not enough actual thread runs!")

        thread_run = wf_run['threadRuns'][output.tr_number]

        if output.task_runs is None:
            continue

        if len(output.task_runs) != len(thread_run['taskRuns']):
            raise RuntimeError("Invalid number of task runs!")

        for i in range(len(output.task_runs or [])):
            answer = output.task_runs[i]
            actual = thread_run['taskRuns'][i]

            if answer.stdout != actual['stdout']:
                breakpoint()
                raise RuntimeError("Mismatched stdout!")


def _get_wf_name(dirname: str):
    for f in os.listdir(f"{dirname}/specs"):
        if not f.endswith('wf.json'):
            continue

        with open(f"{dirname}/specs/{f}", 'r') as handle:
            return json.loads(handle.read())['name']

    raise RuntimeError("Invalid directory, no wfspec file found!")


def run_all_cases(dirname: str):
    wf_name = _get_wf_name(dirname)

    with open(f'{dirname}/check.json', 'r') as f:
        test_cases = json.loads(f.read())

    for item in test_cases:
        test_case = TestCase(**item)
        run_test(wf_name, test_case)

if __name__ == '__main__':
    run_all_cases(sys.argv[1])
