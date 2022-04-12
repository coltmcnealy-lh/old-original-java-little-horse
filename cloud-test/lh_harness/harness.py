import argparse
import json
import os
import time
import sys

import requests

from lh_harness.check_models import Command, TestCase, TestSuite, ThreadRunOutput
from lh_harness.utils import cleanup_case_name


DEFAULT_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")


def run_test(wf_spec_id: str, case: TestCase, url):
    # Step 1: Actually run the workflow.
    wf_run_schema = {
        "wfSpecId": wf_spec_id,
        "variables": case.command.variables,
    }
    run_wf_response = requests.post(f'{url}/WFRun', json=wf_run_schema)
    run_wf_response.raise_for_status()

    wf_run_id = run_wf_response.json()['objectId']

    # Step 2: Wait for it to complete
    time.sleep(case.timeout)

    # Step 3: see if it actually came out properly
    get_wf_response = requests.get(f"{url}/WFRun/{wf_run_id}")
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
                raise RuntimeError("Mismatched stdout!")

        for varname in (output.variables or {}).keys():
            assert output.variables is not None

            if varname not in thread_run['variables']:
                raise RuntimeError("Variable missing!")

            if thread_run['variables'][varname] != output.variables[varname]:
                raise RuntimeError("Mismatched variable!")


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Deploy a test case")

    parser.add_argument(
        "--api-url", '-u', action='store', default=DEFAULT_URL,
        help=f"URL for LittleHorseAPI. Default: {DEFAULT_URL}"
    )
    parser.add_argument(
        "--cases", '-c', nargs='+', default=[],
        help="Names of test cases to test. If left blank, will test all cases."
    )
    parser.add_argument(
        "--requests", "-r", default=1,
        help="Number of requests to send per test case"
    )
    parser.add_argument(
        "--threads", "-t", default=1,
        help="number of concurrent threads to use per test case."
    )

    ns = parser.parse_args()

    if ns.cases is None:
        cases = os.listdir('tests/test_cases')
    else:
        cases = ns.cases

    cases = [cleanup_case_name(case) for case in cases]

    for case in cases:
        with open(case, 'r') as f:
            data = json.loads(f.read())
        test_suite = TestSuite(**data)

        wf_name = test_suite.wf_spec['name']
        for test_case in test_suite.test_cases:
            print("Running test")
            run_test(wf_name, test_case, ns.api_url)
            print("Test didn't crash!")
