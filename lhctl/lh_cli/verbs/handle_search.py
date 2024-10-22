from argparse import _SubParsersAction, ArgumentParser, Namespace
import json
from typing import Any, Generic, Mapping, TypeVar

from lh_lib.client import IndexEntryCollectionSchema, LHClient
from lh_lib.schema import RESOURCE_TYPES
from lh_lib.schema.lh_rpc_response_schema import LHRPCResponseSchema
from lh_lib.schema.wf_run_schema import ThreadRunSchema, WFRunSchema
from lh_lib.schema.wf_spec_schema import (
    WFSpecSchema, 
    ExternalEventDefSchema,
    TaskDefSchema,
)
from lh_lib.utils.printer import Printer


class SEARCHHandler:
    def __init__(self):
        pass

    def init_subparsers(self, base_subparsers: _SubParsersAction):
        parser: ArgumentParser = base_subparsers.add_parser(
            "search",
            help="Search for Resources based on Label Keys and Values."
        )

        parser.add_argument(
            "resource_type",
            choices=[k for k in RESOURCE_TYPES.keys()],
            help="Resource Type to Search."
        )

        parser.add_argument(
            "label_key",
            help="Name of label."
        )
        parser.add_argument(
            "label_value",
            help="Value of label to search for..",
        )

        parser.set_defaults(func=self.search)

    def search(self, ns: Namespace, client: LHClient):
        rt_name: str = ns.resource_type

        rt_schema = RESOURCE_TYPES[rt_name]

        r: LHRPCResponseSchema[IndexEntryCollectionSchema] = client.search_for_alias(
            rt_schema,
            ns.label_key,
            ns.label_value,
        )

        if r.result is None:
            r.result = IndexEntryCollectionSchema(entries=[])

        print(json.dumps([obj.object_id for obj in r.result.entries]))
