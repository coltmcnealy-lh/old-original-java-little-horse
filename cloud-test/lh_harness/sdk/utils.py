"""
This file is a concept prototype implementation that will show the seeds of a cool
SDK for TaskDef creation from actual code.
"""

from inspect import signature, Signature
import json
import os
from typing import Any, Callable

from pydantic import BaseModel as PyThingBaseModel
from humps import camelize
from lh_harness.sdk.wf_spec_schema import WFRunVariableTypeEnum


class LHBaseModel(PyThingBaseModel):
    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True

# Magic so that we have the relevant functions in our "path" or whatever they call
# it in python
from lh_harness.task_implementations import *

DOCKER_IMAGE = os.getenv("LHORSE_TEST_DOCKER_IMAGE", "little-horse-test:latest")


def get_func(func_name) -> Callable:
    return globals()[func_name]


def get_lh_var_type(original_type: Any) -> WFRunVariableTypeEnum:
    if original_type == str:
        return WFRunVariableTypeEnum.STRING
    elif original_type == float:
        return WFRunVariableTypeEnum.DOUBLE
    elif original_type == bool:
        return WFRunVariableTypeEnum.BOOLEAN
    elif original_type == int:
        return WFRunVariableTypeEnum.INT
    elif original_type == dict:
        return WFRunVariableTypeEnum.OBJECT
    elif original_type == list:
        return WFRunVariableTypeEnum.ARRAY
    else:
        raise RuntimeError(f"Bad class type for param: {original_type}")


def cast_all_args(func, *splat_args) -> dict:
    sig: Signature = signature(func)

    args = [thing for thing in splat_args[0]]
    assert len(args) == len(list(sig.parameters.keys()))

    out = {}
    i = 0
    for param_name in sig.parameters.keys():
        arg = args[i]
        i += 1

        param = sig.parameters[param_name]
        assert param.annotation is not None  # we know it's annotated by now

        if param.annotation in [list, dict]:
            out[param_name] = json.loads(arg)
        elif param.annotation == bool:
            out[param_name] = True if arg.lower() == 'true' else False
        else:
            assert param.annotation in [int, float, str]
            out[param_name] = param.annotation(arg)

    return out


def get_task_def_name(func: Callable):
    return func.__name__


def get_task_def(task_def_name):
    task_func = get_func(task_def_name)
    sig: Signature = signature(task_func)

    required_vars = {}

    for param_name in sig.parameters.keys():
        param = sig.parameters[param_name]
        if param.annotation is None:
            raise RuntimeError("You must annotate your parameters!")

        required_vars[param_name] = {
            "type": get_lh_var_type(param.annotation).value
        }

    bash_command = [
        "python",
        "/cloud-test/lh_harness/do_task.py",
        "---THREAD_RUN_ID---",
        "---TASK_RUN_NUMBER---",
        "---WF_RUN_ID---",
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
            }),
            "secondaryValidatorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashValidator",
            "taskExecutorClassName": "little.horse.lib.worker.examples.docker.bashExecutor.BashExecutor",
        }),
        "requiredVars": required_vars,
    }

    return td
