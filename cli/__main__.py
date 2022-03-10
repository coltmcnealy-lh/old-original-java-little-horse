import argparse

from api_client import Client
from wf_spec import WFSpecCLI
from wf_run import WFRunCLI
from task_def import TaskDefCLI
from external_event_def import ExternalEventDefCLI


HANDLERS = {
    'wfSpec': WFSpecCLI(),
    'taskDef': TaskDefCLI(),
    'wfRun': WFRunCLI(),
    'externalEventDef': ExternalEventDefCLI(),
}


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--api-url", help="URL for api")
    subparsers = parser.add_subparsers(dest="resource-type", required=True)

    for cmd in HANDLERS.keys():
        HANDLERS[cmd].init_subparser(subparsers)

    ns = parser.parse_args()

    client = Client(ns.api_url)
    ns.func(client, ns)
