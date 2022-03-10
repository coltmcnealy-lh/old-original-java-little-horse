import argparse

from api_client import Client


class TaskDefCLI:
    def __init__(self):
        pass

    def init_subparser(self, base_subparsers):
        parser = base_subparsers.add_parser(
            "wfSpec",
            help="Tool to create, list, delete, or run TaskDef's."
        )

        # Here we create more subparsers for each of the commands.
        subparsers = parser.add_subparsers(
            dest="resource-action", required=True
        )

        list_parser = subparsers.add_parser(
            "list", help="List TaskDef's"
        )
        list_parser.add_argument(
            "task-def-id", nargs='?', default=None,
            help="TaskDef to get detail on. Leave empty to just list all."
        )
        list_parser.add_argument(
            "--verbose", "-v", action=argparse.BooleanOptionalAction,
            help="get full JSON output.",
            default=False
        )
        list_parser.set_defaults(func=self.list_task_def_def)

        delete_parser = subparsers.add_parser(
            "delete",
            help="Delete aTaskDef"
        )
        delete_parser.add_argument(
            "task-def-id", help="ID of TaskDef to delete."
        )
        delete_parser.set_defaults(func=self.delete_task_def_def)

        add_parser = subparsers.add_parser(
            "add",
            help="Add a WFSpec"
        )
        add_parser.add_argument(
            "file-name", help="text file containing JSON spec for workflow to add."
        )
        add_parser.set_defaults(func=self.add_task_def_def)

    def list_task_def_def(self, client, args):
        print("hello from list task_def")

    def add_task_def_def(self, client, args):
        print("hello from add task_def")

    def delete_task_def_def(self, client, args):
        print("hello from delete task_def")
