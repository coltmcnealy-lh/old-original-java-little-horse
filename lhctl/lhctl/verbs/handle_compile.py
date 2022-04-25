from argparse import ArgumentParser, _SubParsersAction, Namespace
import importlib
import json
from typing import Any, Callable

from lh_lib.client import LHClient
from lh_sdk.compile import get_specs
from lh_sdk.thread_spec_builder import ThreadSpecBuilder, Workflow


class COMPILEHandler():
    def __init__(self):
        pass

    def init_subparsers(self, base_subparsers: _SubParsersAction):
        parser: ArgumentParser = base_subparsers.add_parser(
            "compile",
            help="Compile a LH SDK workflow from python code into JSON Spec files."
        )
        parser.add_argument(
            "module",
            help="Module defining the `workflow` function containing your workflow.",
        )
        parser.add_argument(
            "wf_func",
            help="Name of the workflow entrypoint function.",
        )
        parser.set_defaults(func=self.compile)

    def compile(self, ns: Namespace, client: LHClient):
        # First, import the relevant module so that we have access to the functions.
        importlib.import_module(ns.module)

        # This should work because we should have imported the function in the line
        # above
        wf_func: Callable[[ThreadSpecBuilder], Any] = globals()[ns.wf_func]
        wf = Workflow(wf_func)
        specs = get_specs(wf)

        print(specs.json(by_alias=True))
