import argparse
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import json
import os

import requests
from sqlalchemy import text

from lh_harness.test_case_schema import TestCase, TestSuite
from lh_harness.utils import (
    cleanup_case_name,
    generate_guid,
    get_connection,
    get_file_location,
    DEFAULT_API_URL,
)


def run_wf(wf_spec_id, data, api_url, harness_worker_partition):
    with closing(get_connection()) as session:
        wf_run_id = generate_guid()
        session.execute(
            text("""
                INSERT INTO wf_run (
                    variables,
                    wf_spec_id,
                    wf_run_id,
                    harness_worker_partition
                ) VALUES (
                    :variables,
                    :wf_spec_id,
                    :wf_run_id,
                    :harness_worker_partition
                )
            """),
            wf_spec_id=wf_spec_id, variables=data,
            harness_worker_partition=harness_worker_partition
        )
        wf_run_request = {
            "wfSpecId": wf_spec_id,
            "wfRunId": wf_run_id,
            "variables": json.loads(data),
        }

        response = requests.post(f'{api_url}/WFRun', json=wf_run_request)

        try:
            response.raise_for_status()
            new_status = "LAUNCHED"
            new_code = 0
        except Exception:
            new_status = "FAILED_LAUNCH"
            new_code = -1

        session.execute(
            text("""
                UPDATE wf_run
                SET test_status = :new_status, code = :new_code
                WHERE wf_run_id = :wf_run_id
            """),
            new_status=new_status,
            wf_run_id=wf_run_id,
            new_code=new_code
        )
        print("Successfully ran wf_id", wf_run_id)
        return wf_run_id


def idempotent_pre_setup():
    with closing(get_connection()) as session:
        sql = text("""
        create table if not exists task_run (
            variables varchar(1024),
            start_time timestamp DEFAULT current_timestamp,
            end_time timestamp DEFAULT current_timestamp ON UPDATE current_timestamp,
            wf_run_id varchar(128),
            thread_run_id integer,
            task_run_number integer,
            stdout varchar(1024),
            taskdef varchar(64),
            stderr varchar(1024),
            harness_worker_partition integer
        )
        """)
        session.execute(sql)

        sql = text("""
        CREATE TYPE IF NOT EXISTS test_status_enum AS ENUM(
            'LAUNCHING',
            'LAUNCHED',
            'FAILED_LAUNCH',
            'SUCCEEDED',
            'FAILED_ACCEPTABLE',
            'FAILED_UNACCEPTABLE'
        );
        """)

        sql = text("""
        create table if not exists wf_run (
            variables varchar(1024),
            time timestamp DEFAULT current_timestamp,
            wf_spec_id varchar(128),
            wf_run_id varchar(128),
            message varchar(128),
            status test_status DEFAULT 'LAUNCHING',
            message varchar(1024)
        )
        """)
        session.execute(sql)

        sql = text("""
        create table if not exists get_request (
            success integer,
            time timestamp DEFAULT current_timestamp,
            url varchar(256)
        )
        """)
        session.execute(sql)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description="Launch test cases")

    parser.add_argument(
        "--api-url", '-u', action='store', default=DEFAULT_API_URL,
        help=f"URL for LittleHorseAPI. Default: {DEFAULT_API_URL}"
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
    parser.add_argument(
        "--skip-pre-setup", action="store_true", default=False
    )
    parser.add_argument(
        "--harness-worker-partition", default=0,
        help="partition number for this harness worker"
    )

    ns = parser.parse_args()

    if not ns.skip_pre_setup:
        idempotent_pre_setup()

    if ns.cases is None or len(ns.cases) == 0:
        cases = os.listdir(os.path.join(
            get_file_location(), '..', 'tests')
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
                    wf_name, test_case, ns.api_url, ns.harness_worker_partition
                ) for _ in range(ns.requests)
            ])

    for future in futures:
        future.result()
