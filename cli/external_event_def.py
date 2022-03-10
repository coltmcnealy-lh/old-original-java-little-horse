import argparse

from api_client import Client


class ExternalEventDefCLI:
    def __init__(self):
        pass

    def init_subparser(self, base_subparsers):
        parser = base_subparsers.add_parser(
            "wfSpec",
            help="Tool to create, list, delete, or run ExternalEventDef's."
        )

        # Here we create more subparsers for each of the commands.
        subparsers = parser.add_subparsers(
            dest="resource-action", required=True
        )

        list_parser = subparsers.add_parser(
            "list", help="List ExternalEventDef's"
        )
        list_parser.add_argument(
            "external-event-id", nargs='?', default=None,
            help="ExternalEventDef to get detail on. Leave empty to just list all."
        )
        list_parser.add_argument(
            "--verbose", "-v", action=argparse.BooleanOptionalAction,
            help="get full JSON output.",
            default=False
        )
        list_parser.set_defaults(func=self.list_external_event_def)

        delete_parser = subparsers.add_parser(
            "delete",
            help="Delete an ExternalEventDef"
        )
        delete_parser.add_argument(
            "external-event-id", help="ID of ExternalEventDef to delete."
        )
        delete_parser.set_defaults(func=self.delete_external_event_def)

        add_parser = subparsers.add_parser(
            "add",
            help="Add a WFSpec"
        )
        add_parser.add_argument(
            "file-name", help="text file containing JSON spec for workflow to add."
        )
        add_parser.set_defaults(func=self.add_external_event_def)

    def list_external_event_def(self, client, args):
        print("hello from list eed")

    def add_external_event_def(self, client, args):
        print("hello from add eed")

    def delete_external_event_def(self, client, args):
        print("hello from delete eed")


