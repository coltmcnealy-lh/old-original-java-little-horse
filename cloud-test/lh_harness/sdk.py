"""
This file is a concept prototype implementation that will show the seeds of a cool
SDK for TaskDef creation from actual code.
"""

from inspect import signature, Signature
import json
import os
from typing import Any, Callable


DOCKER_IMAGE = os.getenv("LHORSE_TEST_DOCKER_IMAGE", "little-horse-test:latest")


def my_func(a: str, b: int, c: list = []) -> list:
    return ["asdf"]


def get_func(func_name) -> Callable:
    return globals()[func_name]


def get_lh_var_type(original_type: Any):
    if original_type == str:
        return "STRING"
    elif original_type == float:
        return "DOUBLE"
    elif original_type == bool:
        return "BOOLEAN"
    elif original_type == int:
        return "INTEGER"
    elif original_type == dict:
        return "OBJECT"
    elif original_type == list:
        return "ARRAY"
    else:
        raise RuntimeError(f"Bad class type for param: {original_type}")


def get_task_def(task_def_name):
    task_func = get_func(task_def_name)
    sig: Signature = signature(task_func)

    required_vars = {}

    for param_name in sig.parameters.keys():
        param = sig.parameters[param_name]
        if param.annotation is None:
            raise RuntimeError("You must annotate your parameters!")

        required_vars[param_name] = {
            "type": get_lh_var_type(param.annotation)
        }

    bash_command = [
        "python",
        "/cloud-test/lh_harness/do_task.py",
        "<<<<THREAD_RUN_ID>>>>",
        "<<<<TASK_RUN_NUMBER>>>>",
        "<<<<WF_RUN_ID>>>>",
        task_def_name,
    ]

    for varname in required_vars.keys():
        bash_command.append(f"<<{varname}>>")

    td = {
        "name": task_def_name,
        "deployMetadata": json.dumps({
            "dockerImage": DOCKER_IMAGE,
            "metadata": json.dumps({
                "bashCommand": bash_command,
                "secondaryValidatorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashValidator",
                "taskExecutorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashExecutor"
            })
        })
    }

    return td


if __name__ == '__main__':
    get_task_def('my_func')
