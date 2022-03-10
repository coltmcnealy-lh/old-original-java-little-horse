import argparse

from api_client import Client


class WFSpecCLI:
    def __init__(self):
        pass

    def init_subparser(self, base_subparsers):
        parser = base_subparsers.add_parser(
            "wfSpec",
            help="Tool to create, list, delete, or run WFSpec's."
        )

        # Here we create more subparsers for each of the commands.
        subparsers = parser.add_subparsers(
            dest="resource-action", required=True
        )

        list_parser = subparsers.add_parser(
            "list", help="List WFSpec's"
        )
        list_parser.add_argument(
            "wf-spec-id", nargs='?', default=None,
            help="Specific WFSpec to get detail on. Leave empty to just list all."
        )
        list_parser.add_argument(
            "--verbose", "-v", action=argparse.BooleanOptionalAction,
            help="get full JSON output.",
            default=False
        )
        list_parser.set_defaults(func=self.list_wf)

        run_parser = subparsers.add_parser(
            "run",
            help="Run a wfSpec"
        )
        run_parser.add_argument("wf-spec-id", help="WFSpec to run")
        run_parser.add_argument(
            "data", default=None,
            help="data to pass as input variables to the workflow. JSON recommended."
        )
        run_parser.set_defaults(func=self.run_wf)

        delete_parser = subparsers.add_parser(
            "delete",
            help="Delete a WFSpec"
        )
        delete_parser.add_argument("wf-spec-id", help="ID of wf spec to delete.")
        delete_parser.set_defaults(func=self.delete_wf)

        add_parser = subparsers.add_parser(
            "add",
            help="Add a WFSpec"
        )
        add_parser.add_argument(
            "file-name", help="text file containing JSON spec for workflow to add."
        )
        add_parser.set_defaults(func=self.add_wf)

    def list_wf(self, client, args):
        print("hello from list wf")

    def run_wf(self, client, args):
        print("hello from run_wf")

    def add_wf(self, client, args):
        print("hello from add_wf")

    def delete_wf(self, client, args):
        print("hello from delete_wf")
