from argparse import ArgumentParser, _SubParsersAction, Namespace
import importlib
from typing import Any, Callable

from lh_lib.client import LHClient
from lh_sdk.compile import get_specs
from lh_sdk.thread_spec_builder import ThreadSpecBuilder, Workflow


class BUILDHandler():
    def __init__(self):
        pass

    def init_subparsers(self, base_subparsers: _SubParsersAction):
        parser: ArgumentParser = base_subparsers.add_parser(
            "build",
            help="[Prototype] Build docker images specified by `lhctl compile`."
        )
        parser.add_argument(
            "spec_file",
            help="File containing the output of `lhctl compile`.",
        )
        parser.set_defaults(func=self.build)

    def build(self, ns: Namespace, client: LHClient):
        # First, import the relevant module so that we have access to the functions.
        mod = importlib.import_module(ns.module)

        # This should work because we should have imported the function in the line
        # above
        wf_func: Callable[[ThreadSpecBuilder], Any] = mod.__dict__[ns.wf_func]
        wf = Workflow(wf_func, mod.__dict__)
        specs = get_specs(wf)

        print(specs.json(by_alias=True))
