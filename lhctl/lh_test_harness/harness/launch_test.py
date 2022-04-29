import importlib
from types import ModuleType
from typing import Callable, List

from lh_lib.client import LHClient

from lh_test_harness.test_client import TestClient
from lh_test_harness.test_utils import (
    get_test_module_name,
)


def get_launch_funcs(
    test_name: str,
    mod: ModuleType
) -> List[Callable[[TestClient, str], None]]:
    out = []

    for key in mod.__dict__.keys():
        if key.startswith(f'launch_{test_name}'):
            out.append(mod.__dict__[key])

    return out


def launch_test(test_name: str, client: TestClient):
    test_module_name = get_test_module_name(test_name)
    mod: ModuleType = importlib.import_module(test_module_name)

    launch_funcs = get_launch_funcs(test_name, mod)

    for f in launch_funcs:
        f(client, test_name)


if __name__ == '__main__':
    client = LHClient()
    test_client = TestClient(client)
    launch_test("basic", test_client)
