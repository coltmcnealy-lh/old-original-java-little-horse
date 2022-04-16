import argparse
from contextlib import closing
import json
import os
from typing import Callable

from lh_harness.sdk.sdk import cast_all_args, get_func

# # Make all the task implementation functions available for the voodoo we do.
# from lh_harness.task_implementations import *


def execute_task(thread_run_id, task_run_number, wf_run_id, task_def_name, *args):
    func: Callable = get_func(task_def_name)

    new_args = cast_all_args(func, args)

    result = func(*new_args)
    if isinstance(result, list) or isinstance(result, dict):
        result = json.dumps(result)
    print(result, end='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("thread_run_id")
    parser.add_argument("task_run_number")
    parser.add_argument("wf_run_id")
    parser.add_argument("task_def_name")

    ns, unknown = parser.parse_known_args()

    execute_task(
        ns.thread_run_id, ns.task_run_number, ns.wf_run_id,
        ns.task_def_name, *unknown
    )

