import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os
from typing import Iterable, List, Optional, Tuple

import requests
from sqlalchemy import text
from lh_harness.db_schema import TaskRun, TestStatus, WFRun

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

    return check_task_runs_and_thread_runs(wf_run_orm, wf_run)


def iter_all_task_runs(wf_run: dict) -> Iterable[Tuple[int, dict]]:
    for i, thread_run in enumerate(wf_run['threadRuns']):
        for task_run in thread_run['taskRuns']:
            if task_run['nodeType'] != 'TASK':
                continue
            yield i, task_run


def find_task_run(wf_run_orm, thr_num, tr_num) -> Optional[TaskRun]:
    task_runs: List[TaskRun] = wf_run_orm.task_runs

    for task_run in task_runs:
        if task_run.thread_run_id == thr_num and task_run.task_run_number == tr_num:
            return task_run

    return None


def check_task_runs_and_thread_runs(wf_run_orm, wf_run):
    orphans = []
    mis_reports = []
    for thr_num, task_run in iter_all_task_runs(wf_run):
        # make sure that each task_run:
        # 1. Actually got executed and recorded in the db
        # 2. Matches the output in the db
        assert thr_num == task_run['threadID']
        tr_orm = find_task_run(
            wf_run_orm, task_run['threadID'], task_run['number']
        )

        if tr_orm == None:
            if task_run['status'] == 'FAILED':
                # This means that the task got scheduled but the worker was down.
                orphans.append(task_run)
                continue
            else:
                breakpoint()
                # This means that LittleHorse hallucinated about the task and thought
                # it got done but the task never did get done (either that, or the
                # database decided to drop a record, which shouldn't be possible)
                return (
                    TestStatus.FAILED_UNACCEPTABLE,
                    "Phantom task run that wasn't found in database!"
                )

        if tr_orm.stdout != task_run['stdout']:
            if task_run['status'] == 'FAILED':
                mis_reports.append(task_run)
            else:
                return (
                    TestStatus.FAILED_UNACCEPTABLE,
                    "DB and LH show different stdouts!"
                )

    wf_run_orm.num_mis_reported = len(mis_reports)
    wf_run_orm.num_orphans = len(orphans)

    if len(orphans) == 0 and len(mis_reports) == 0:
        return TestStatus.SUCCEEDED, "The Force is with us!"

    return TestStatus.FALIED_ACCEPTABLE, "Had some minor reporting errors"


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

