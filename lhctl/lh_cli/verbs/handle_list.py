from argparse import _SubParsersAction, ArgumentParser, Namespace
import time
from typing import Optional

from lh_lib.client import RangeQueryResultSchema, LHClient
from lh_lib.schema import RESOURCE_TYPES
from lh_lib.schema.lh_rpc_response_schema import LHRPCResponseSchema



class LISTHandler:
    def __init__(self):
        pass

    def init_subparsers(self, base_subparsers: _SubParsersAction):
        parser: ArgumentParser = base_subparsers.add_parser(
            "list",
            help="List all resources or resources created within a time window."
        )

        parser.add_argument(
            "resource_type",
            choices=[k for k in RESOURCE_TYPES.keys()],
            help="Resource Type to Search."
        )

        parser.add_argument(
            "--since-seconds", "-s", type=int,
            help="Number of seconds ago that resources should have been created."
        )

        parser.add_argument(
            "--from-timestamp", "-f", type=int,
            help="Time in long (millis) format from which to start iterating."
        )
        parser.add_argument(
            "--to-timestamp", "-t", type=int,
            help="Time in long (millis) format until which to continue iterating."
        )

        parser.add_argument(
            "--limit", "-l", type=int, default=10,
            help="Limit number of records for this page."
        )
        parser.add_argument(
            "--token", "-t",
            help="Iterator token for paginated searches."
        )
        parser.add_argument(
            "--raw-json", "-r", action="store_true",
            help="Print raw json result"
        )

        parser.set_defaults(func=self.list)

    def list(self, ns: Namespace, client: LHClient):
        rt_name: str = ns.resource_type
        rt_schema = RESOURCE_TYPES[rt_name]

        start: Optional[int] = None
        end: Optional[int] = None
        
        if ns.since_seconds is not None:
            start = int(time.time()) - ns.since_seconds
        else:
            start = ns.from_timestamp
            end = ns.to_timestamp

        r: LHRPCResponseSchema[RangeQueryResultSchema] = client.list(
            rt_schema,
            start,
            end,
            token=ns.token,
            limit=ns.limit,
        )

        if r.result is None:
            r.result = RangeQueryResultSchema(object_ids=[])

        if ns.raw_json:
            print(r.result.json(by_alias=True))
        else:
            if r.result.object_ids is None or len(r.result.object_ids) == 0:
                print("No objects found.")
            else:
                for obj_id in r.result.object_ids:
                    print(obj_id)
