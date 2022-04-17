import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from typing import Tuple

import requests
from sqlalchemy import text

from lh_harness.utils.test_case_schema import TestCase, TestSuite
from lh_harness.utils.utils import (
    are_equal,
    cleanup_case_name,
    get_root_dir,
    DEFAULT_API_URL,
)


def validate_wf_run(wf_run: dict, case: TestCase) -> Tuple[str, str]:
    test_run_id = wf_run['objectId']
    for output in case.output:
        if output.tr_number >= len(wf_run['threadRuns']):
            return "FAILED_UNACCEPTABLE", "Not enough threadruns in actual wf!"

        thread_run = wf_run['threadRuns'][output.tr_number]

        if output.task_runs is not None:
            if len(output.task_runs) != len(thread_run['taskRuns']):
                return (
                    "FAILED_UNACCEPTABLE",
                    f"Wanted {len(output.task_runs)} taskruns," +
                    f" not {len(thread_run['taskRuns'])}"
                )

            for i in range(len(output.task_runs or [])):
                answer = output.task_runs[i]
                actual = thread_run['taskRuns'][i]

                if not are_equal(answer.stdout, actual['stdout']):
                    return "FALIED_UNACCEPTABLE", "Mismatched stdout!"

        for varname in (output.variables or {}).keys():
            assert output.variables is not None

            if varname not in thread_run['variables']:
                return (
                    "FAILED_UNACCEPTABLE",
                    f"{varname} not defined for ThreadRun."
                )

            if not are_equal(
                thread_run['variables'][varname], output.variables[varname]
            ):
                return (
                    "FAILED_UNACCEPTABLE",
                    f"Variable {varname} had wrong value."
                )

    return "SUCCEEDED", "The Force is with us!"


def iter_all_wf_runs():
    while True:
        yield {}


def check_test_case(test_case: TestCase, api_url: str, test_suite: TestSuite):
    # Need to iterate through all of the WFRun's and check them.
    wf_name = test_suite.wf_spec['name']

    for wf_run in iter_all_wf_runs():
        ############# TODO: Actually implement this and the above
        pass


def check_test_suite(case_file: str, api_url: str):
    with open(case_file, 'r') as f:
        data = json.loads(f.read())
    test_suite = TestSuite(**data)

    for test_case in test_suite.test_cases:
        check_test_case(test_case, api_url, test_suite)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launch test cases")

    parser.add_argument(
        "--api-url", '-u', action='store', default=DEFAULT_API_URL,
        help=f"URL for LittleHorseAPI. Default: {DEFAULT_API_URL}"
    )
    parser.add_argument(
        "--threads", "-t", default=1,
        help="number of concurrent threads to use per test case."
    )
    parser.add_argument(
        "--harness-worker-partition", default=0,
        help="partition number for this harness worker"
    )

    ns = parser.parse_args()

    if len(ns.cases) == 0:
        cases = os.listdir(os.path.join(
            get_root_dir(), 'tests')
        )
    else:
        cases = ns.cases

    cases = [cleanup_case_name(case) for case in cases]
