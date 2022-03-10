import argparse

from api_client import Client


class WFRunCLI:
    def __init__(self):
        pass

    def init_subparser(self, base_subparsers):
        parser = base_subparsers.add_parser(
            "wfSpec",
            help="Tool to create, list or delete WFRun's."
        )

        # Here we create more subparsers for each of the commands.
        subparsers = parser.add_subparsers(
            dest="resource-action", required=True
        )

        list_parser = subparsers.add_parser(
            "list", help="List WFRun's"
        )
        list_parser.add_argument(
            "wf-run-id", nargs='?', default=None,
            help="Specific WFRun to get detail on. Leave empty to just list all."
        )
        list_parser.add_argument(
            "--verbose", "-v", action=argparse.BooleanOptionalAction,
            help="get full JSON output.",
            default=False
        )
        list_parser.set_defaults(func=self.list_wf_run)

        delete_parser = subparsers.add_parser(
            "delete",
            help="Delete a WFRun"
        )
        delete_parser.add_argument("wf-run-id", help="ID of wf run to delete.")
        delete_parser.set_defaults(func=self.delete_wf_run)

    def list_wf_run(self, client, args):
        print("hello from list wfrun")

    def delete_wf_run(self, client, args):
        print("hello from delete_wfrun")
