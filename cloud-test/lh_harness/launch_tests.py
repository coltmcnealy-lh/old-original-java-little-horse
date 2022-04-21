import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os

import requests
from sqlalchemy import text
from sqlalchemy.orm.session import Session

from lh_harness.test_case_schema import TestCase, TestSuite
from lh_harness.db_schema import (
    TestStatus,
    WFRun,
)
from lh_harness.utils import (
    cleanup_case_name,
    generate_guid,
    get_session,
    get_root_dir,
    DEFAULT_API_URL,
)


def run_wf(wf_spec_id, data, api_url, harness_worker_partition):
    with closing(get_session()) as session:
        run_wf_helper(wf_spec_id, data, api_url, harness_worker_partition, session)


def run_wf_helper(
    wf_spec_id: str,
    data: dict,
    api_url: str,
    harness_worker_partition: int,
    session: Session
):
    wf_run_id = generate_guid()

    wf_run = WFRun(
        wf_run_id=wf_run_id,
        wf_spec_id=wf_spec_id,
        variables=data,
        status=TestStatus.LAUNCHING,
        harness_worker_partition=harness_worker_partition,
    )

    session.add(wf_run)
    session.commit()

    wf_run_request = {
        "wfSpecId": wf_spec_id,
        "wfRunId": wf_run_id,
        "variables": data,
    }

    response = requests.post(f'{api_url}/WFRun', json=wf_run_request)

    try:
        response.raise_for_status()
        new_status = TestStatus.LAUNCHED
        message = None
    except Exception:
        new_status = TestStatus.FAILED_LAUNCH
        message = "Failed launching the WFRun!"

    wf_run.status = new_status
    wf_run.message = message
    session.merge(wf_run)
    session.commit()

    print("Successfully ran wf_id", wf_run_id)
    return wf_run_id


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launch test cases")

    parser.add_argument(
        "--api-url", '-u', action='store', default=DEFAULT_API_URL,
        help=f"URL for LittleHorseAPI. Default: {DEFAULT_API_URL}"
    )
    parser.add_argument(
        "--requests", "-r", default=1,
        help="Number of requests to send per test case"
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
        "cases", nargs='?', default=[],
        help="Names of test cases to test. If left blank, will test all cases."
    )

    ns = parser.parse_args()

    if ns.cases is None or len(ns.cases) == 0:
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
            futures.extend([
                executor.submit(
                    run_wf,
                    wf_name, test_case.command.variables, ns.api_url,
                    ns.harness_worker_partition
                ) for _ in range(ns.requests)
            ])

    for future in futures:
        future.result()
