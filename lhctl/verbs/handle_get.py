from argparse import _SubParsersAction, ArgumentParser, Namespace
import json
from typing import Any, Mapping

from lhctl.client import LHClient
from lhctl.schema import RESOURCE_TYPES
from lhctl.schema.lh_rpc_response_schema import LHRPCResponseSchema


class GETWFRun:
    pass

class GetWFSpec:
    pass


GETTABLE_RESOURCES: Mapping[str, Any] = {
    "WFRun": GETWFRun(),
    "WFSpec": GetWFSpec(),
}


class GETHandler:
    def __init__(self):
        pass

    def init_subparsers(self, base_subparsers: _SubParsersAction):
        parser: ArgumentParser = base_subparsers.add_parser(
            "get",
            help="Get information about a specified Resource Name and Resource Type."
        )

        parser.add_argument(
            "resource_type",
            choices=[k for k in GETTABLE_RESOURCES.keys()],
            help="Resource Type to List, Get, or Search."
        )

        parser.add_argument(
            "resource_id",
            help="Specific Id or Name of resource to get."
        )

        parser.set_defaults(func=self.get_resource)

    def get_resource(self, ns: Namespace, client: LHClient):
        rt_name: str = ns.resource_type
        resource_id: str = ns.resource_id

        rt_schema = RESOURCE_TYPES[rt_name]

        response: LHRPCResponseSchema[rt_schema] = client.get_resource_by_id(
            rt_schema,
            resource_id,
        )

        print(response.json(by_alias=True))
