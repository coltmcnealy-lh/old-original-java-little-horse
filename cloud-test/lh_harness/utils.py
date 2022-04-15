import hashlib
from inspect import getsourcefile
import os
import uuid

from sqlalchemy import create_engine


DEFAULT_API_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")

POSTGRES_URI = os.getenv(
    "DB_URI",
    "postgresql://postgres:postgres@localhost:5432/postgres"
)

engine = create_engine(POSTGRES_URI)


def get_connection():
    return engine.connect()


def generate_guid():
    return uuid.uuid4().hex


def get_file_location():
    this_file = getsourcefile(lambda: 0)
    assert this_file is not None
    dir_of_this_file = os.path.split(this_file)[0]
    return dir_of_this_file


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
