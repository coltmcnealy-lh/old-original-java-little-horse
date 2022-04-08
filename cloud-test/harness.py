from contextlib import closing
import json
import os
import sys
import uuid

import requests
from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import sessionmaker



POSTGRES_URI = os.getenv(
    "DB_URI",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)

LH_URL = os.getenv(
    "LHORSE_API_URL",
    "http://localhost:5000/"
)
if not LH_URL.endswith('/'):
    LH_URL += "/"

engine = create_engine(POSTGRES_URI)


def get_connection():
    return engine.connect()


def idempotent_pre_setup():
    with closing(get_connection()) as session:
        sql = text("""
        create table if not exists task_run (
            variables varchar(1024),
            time timestamp DEFAULT current_timestamp,
            wf_run_id varchar(128),
            thread_run_id integer,
            task_run_number integer,
            stdout varchar(1024),
            taskdef varchar(64),
            stderr varchar(1024)
        )
        """)
        session.execute(sql)

        sql = text("""
        create table if not exists wf_run (
            variables varchar(1024),
            time timestamp DEFAULT current_timestamp,
            wf_spec_id varchar(128)
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


def run_wf(wf_spec_id, data):
    with closing(get_connection()) as session:
        session.execute(
            text("""
                INSERT INTO wf_run (variables, wf_spec_id) VALUES (
                    :variables,
                    :wf_spec_id
                )
            """),
            wf_spec_id=wf_spec_id, variables=data
        )
        wf_run_request = {
            "wfSpecId": wf_spec_id,
            "variables": json.loads(data),
        }
        requests.post(f'{LH_URL}/WFRun', json=wf_run_request).raise_for_status()


def run_task(wf_run, thread_run, task_run, data):
    task_def_id = int(os.getenv("TASK_DEF_ID", "1"))
    output = "Running a task!"
    with closing(get_connection()) as session:
        session.execute(
            text("""
                INSERT INTO task_run (
                    variables,
                    wf_run_id,
                    thread_run_id,
                    task_run_number,
                    stdout,
                    taskdef,
                    stderr
                ) VALUES (
                    :variables,
                    :wf_run_id,
                    :thread_run_id,
                    :task_run_id,
                    :output,
                    :task_def,
                    :stderr
                )
            """),
            variables=data, wf_run_id=wf_run, thread_run_id=thread_run,
            task_run_id=task_run, output=output, task_def=task_def_id,
            stderr=""
        )
        print(output)


if __name__ == '__main__':

    if sys.argv[1] == 'setup':
        print("setting up")
        idempotent_pre_setup()
        print("done setting up")

    elif sys.argv[1] == 'wfrun':
        wf_spec_id = sys.argv[2]
        if len(sys.argv) == 4:
            data = sys.argv[3]
        else:
            data = ''
        run_wf(wf_spec_id, data)

    elif sys.argv[1] == 'taskrun':
        wf_run_id = sys.argv[2]
        thread_run_id = int(sys.argv[3])
        task_run_id = int(sys.argv[4])
        if len(sys.argv) == 6:
            data = sys.argv[5]
        else:
            data = {}
        run_task(wf_run_id, thread_run_id, task_run_id, data)
