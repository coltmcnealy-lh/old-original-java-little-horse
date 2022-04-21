import argparse

from examples.basic_wf import *
from lh_sdk.utils import cast_all_args


def execute_task(task_def_name: str, *args):
    func = globals()[task_def_name]
    new_args = cast_all_args(func, *args)

    result = func(*list(new_args.values()))
    print(result, end='')


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("task_def_name")

    ns, unknown = parser.parse_known_args()
    execute_task(ns.task_def_name, *unknown)
