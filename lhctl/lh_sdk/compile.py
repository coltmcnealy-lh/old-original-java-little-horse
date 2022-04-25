from inspect import signature, Signature
import json
import time
from typing import Iterable, List, Set

import os
from humps import camelize

from lh_sdk.thread_spec_builder import Workflow
from lh_sdk.utils import LHBaseModel, add_resource, get_lh_var_type
from lh_lib.schema.wf_spec_schema import (
    ExternalEventDefSchema,
    TaskDefSchema,
    WFSpecSchema,
    NodeSchema,
    NodeType,
)


DEFAULT_API_URL = os.getenv("LHORSE_API_URL", "http://localhost:5000")
DEFAULT_DOCKER_IMAGE = os.getenv(
    "LHORSE_TEST_DOCKER_IMAGE",
    "little-horse-test:latest"
)

SECONDARY_VAL = "little.horse.lib.worker.examples.docker.bashExecutor.BashValidator"
EXECUTOR_CLASS = "little.horse.lib.worker.examples.docker.bashExecutor.BashExecutor"

def iter_nodes(wf: WFSpecSchema) -> Iterable[NodeSchema]:
    for tspec_name in wf.thread_specs.keys():
        tspec = wf.thread_specs[tspec_name]
        for node_name in tspec.nodes.keys():
            node = tspec.nodes[node_name]
            yield node


def get_task_defs_for_wf(spec: WFSpecSchema) -> Set[str]:
    out = set({})
    for node in iter_nodes(spec):
        if node.node_type != NodeType.TASK:
            continue
        out.add(node.task_def_name)
    return out


def get_external_events_for_wf(spec: WFSpecSchema) -> Set[str]:
    out = set({})
    for node in iter_nodes(spec):
        if node.node_type != NodeType.EXTERNAL_EVENT:
            continue
        out.add(node.external_event_def_name)
    return out


# TODO: This should return a BaseModel not a raw dict
def create_external_event_def(name: str) -> dict:
    return {"name": name}


# TODO: This should return a BaseModel, not a raw dict
def create_task_def(task_def_name: str) -> dict:
    task_func = globals()[task_def_name]

    sig: Signature = signature(task_func)

    required_vars = {}

    for param_name in sig.parameters.keys():
        param = sig.parameters[param_name]
        if param.annotation is None:
            raise RuntimeError("You must annotate your parameters!")

        required_vars[param_name] = {
            "type": get_lh_var_type(param.annotation).value
        }

    bash_command = [
        "python",
        "/lh-sdk-python/execute_task.py",
        task_def_name,
    ]

    for varname in required_vars.keys():
        bash_command.append(f"<<{varname}>>")

    task_def = {
        "name": task_def_name,
        "deployMetadata": json.dumps({
            "dockerImage": DEFAULT_DOCKER_IMAGE,
            "metadata": json.dumps({
                "bashCommand": bash_command,
            }),
            "secondaryValidatorClassName": SECONDARY_VAL,
            "taskExecutorClassName": EXECUTOR_CLASS,
        }),
        "requiredVars": required_vars,
    }
    return task_def


def _spec_result_alias_generator(s: str) -> str:
    return {
        'externalEventDef': "ExternalEventDef",
        'taskDef': "TaskDef",
        'wfRun': "WFRun",
        "wfSpec": "WFSpec",
        "dockerfile": "Dockerfile"
    }.get(camelize(s), camelize(s))


class SpecsResult(LHBaseModel):
    external_event_def: List[ExternalEventDefSchema]
    task_def: List[TaskDefSchema]
    wf_spec: List[WFSpecSchema]
    dockerfile: List[str]

    class Config:
        alias_generator = _spec_result_alias_generator


def get_specs(wf: Workflow):
    task_def_names = get_task_defs_for_wf(wf.spec)
    events = get_external_events_for_wf(wf.spec)

    return SpecsResult(**{
        'ExternalEventDef': [create_external_event_def(e) for e in events],
        'TaskDef': [create_task_def(t) for t in task_def_names],
        'WFSpec': [json.loads(wf.spec.json(by_alias=True))]
    })
