from inspect import getsourcefile, signature, Signature
import json
import os
from typing import Callable
import uuid

from lh_sdk.utils import get_lh_var_type
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from sqlalchemy.orm.session import sessionmaker

# Magic so that we have the relevant functions in our "path" or whatever they call
# it in python
from lh_harness.task_implementations import *

POSTGRES_URI = os.getenv(
    "DB_URI",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)

_engine = None
_session_maker = None

def get_session() -> Session:
    global _engine
    global _session_maker

    if _engine is None:
        assert _session_maker is None
        _engine = create_engine(POSTGRES_URI)
        _session_maker = sessionmaker(bind=_engine)

    return _session_maker() # type: ignore

DEFAULT_API_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")
DOCKER_IMAGE = os.getenv("LHORSE_TEST_DOCKER_IMAGE", "little-horse-test:latest")


def generate_guid() -> str:
    return uuid.uuid4().hex


def get_func(func_name) -> Callable:
    return globals()[func_name]


def get_root_dir():
    this_file = getsourcefile(lambda: 0)
    assert this_file is not None
    dir_of_this_file = os.path.split(this_file)[0]
    return os.path.join(dir_of_this_file, '..')


def cleanup_case_name(case):
    if not case.endswith('.json'):
        case += '.json'

    this_file = getsourcefile(lambda: 0)
    assert this_file is not None

    dir_of_this_file = os.path.split(this_file)[0]
    test_dir = os.path.join(
        dir_of_this_file,
        "../tests/"
    )
    case = os.path.join(test_dir, os.path.split(case)[1])

    return case


def are_equal(var1, var2):
    if var1 is None and var2 is None:
        return True

    if var1 is not None and var2 is None:
        return False

    if var2 is not None and var1 is None:
        return False

    if type(var1) != type(var2):
        return False

    if type(var1) in [str, int, bool, float]:
        return var1 == var2

    if type(var1) == list:
        if len(var1) != len(var2):
            return False

        for i in range(len(var1)):
            if not are_equal(var1[i], var2[i]):
                return False
        return True

    assert type(var1) == dict

    if len(list(var1.keys())) != len(list(var2.keys())):
        return False

    for k in var1.keys():
        if k not in var2:
            return False
        if not are_equal(var1[k], var2[k]):
            return False
    return True


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
