import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from typing import Tuple

import requests
from sqlalchemy import text
from lh_harness.db_schema import TestStatus, WFRun

from lh_harness.utils.test_case_schema import TestCase, TestSuite
from lh_harness.utils.utils import (
    are_equal,
    cleanup_case_name,
    get_root_dir,
    DEFAULT_API_URL,
    get_session,
)


def request_wf_run(wf_run_guid, url):
    req_url = f"{url}/WFRun/{wf_run_guid}"
    response = requests.get(req_url)
    response.raise_for_status()
    return response.json()['result']


def validate_wf_run(
    wf_run_orm: WFRun,
    test_case: TestCase,
    url: str
):
    new_status, new_message = validate_wf_run_helper(wf_run_orm, test_case, url)
    wf_run_orm.status = new_status
    wf_run_orm.message = new_message
    wf_run_orm.already_graded = True


def validate_wf_run_helper(
    wf_run_orm: WFRun,
    test_case: TestCase,
    url: str
) -> Tuple[TestStatus, str]:
    try:
        wf_run: dict = request_wf_run(wf_run_orm.wf_run_id, url)
    except Exception as exn:
        return (
            TestStatus.FAILED_UNACCEPTABLE,
            "Had orzdash when looking up: " + str(exn)
        )

    if wf_run is None:
        return (
            TestStatus.FAILED_UNACCEPTABLE,
            "Couldn't find WFRun in the API."
        )

    for output in test_case.output:
        if output.tr_number >= len(wf_run['threadRuns']):
            return (
                TestStatus.FAILED_UNACCEPTABLE,
                "Not enough threadruns in actual wf!"
            )

        thread_run = wf_run['threadRuns'][output.tr_number]

        if output.task_runs is not None:
            if len(output.task_runs) != len(thread_run['taskRuns']):
                return (
                    TestStatus.FAILED_UNACCEPTABLE,
                    f"Wanted {len(output.task_runs)} taskruns," +
                    f" not {len(thread_run['taskRuns'])}"
                )

            for i in range(len(output.task_runs or [])):
                answer = output.task_runs[i]
                actual = thread_run['taskRuns'][i]

                if not are_equal(answer.stdout, actual['stdout']):
                    return TestStatus.FAILED_UNACCEPTABLE, "Mismatched stdout!"

        for varname in (output.variables or {}).keys():
            assert output.variables is not None

            if varname not in thread_run['variables']:
                return (
                    TestStatus.FAILED_UNACCEPTABLE,
                    f"{varname} not defined for ThreadRun."
                )

            if not are_equal(
                thread_run['variables'][varname], output.variables[varname]
            ):
                return (
                    TestStatus.FAILED_UNACCEPTABLE,
                    f"Variable {varname} had wrong value."
                )

    return TestStatus.SUCCEEDED, "The Force is with us!"


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


def check_all_runs(
    executor: ThreadPoolExecutor,
    test_case: TestCase,
    wf_spec_name: str,
    url: str,
    partition: int,
):
    with closing(get_session()) as session:
        q = session.query(
            WFRun
        ).filter(
            WFRun.wf_spec_id == wf_spec_name
        ).filter(
            WFRun.already_graded != True
        ).filter(
            WFRun.harness_worker_partition == partition
        )

        for result in q.all():
            validate_wf_run(
                result,
                test_case,
                url
            )
            session.merge(result)
            session.commit()


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
    parser.add_argument(
        "--cases", '-c', nargs='+', default=[],
        help="Names of test cases to test. If left blank, will test all cases."
    )

    ns = parser.parse_args()

    if len(ns.cases) == 0:
        cases = os.listdir(os.path.join(
            get_root_dir(), 'tests')
        )
    else:
        cases = ns.cases

    cases = [cleanup_case_name(case) for case in cases]

    futures = []

    for case in cases:
        executor = ThreadPoolExecutor(max_workers=ns.threads)
        with open(case, 'r') as f:
            data = json.loads(f.read())
        test_suite = TestSuite(**data)

        wf_name = test_suite.wf_spec['name']
        for test_case in test_suite.test_cases:
            check_all_runs(
                executor,
                test_case,
                wf_name,
                ns.api_url,
                ns.harness_worker_partition,
            )

    for future in futures:
        future.result()

