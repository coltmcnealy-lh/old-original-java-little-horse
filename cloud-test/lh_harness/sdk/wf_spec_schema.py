from enum import Enum
import json
from tkinter.messagebox import NO
from typing import Any, List, Mapping, Optional

from pydantic import Field

from lh_harness.sdk.utils import LHBaseModel


class LHDeployStatus(Enum):
    STARTING = 'STARTING'
    RUNNING = 'RUNNING'
    COMPLETED = 'COMPLETED'
    STOPPING = 'STOPPING'
    STOPPED = 'STOPPED'
    DESIRED_REDEPLOY = 'DESIRED_REDEPLOY'
    ERROR = 'ERROR'


class WFRunVariableTypeEnum(Enum):
    OBJECT = 'OBJECT'
    ARRAY = 'ARRAY'
    INT = 'INT'
    DOUBLE = 'DOUBLE'
    BOOLEAN = 'BOOLEAN'
    STRING = 'STRING'


class WFRunVariableDef(LHBaseModel):
    type: WFRunVariableTypeEnum
    default_value: Optional[Any] = None


class InterruptDef(LHBaseModel):
    handler_thread_name: str


class WFRunMetadataEnum(Enum):
    THREAD_GUID = 'THREAD_GUID'
    THREAD_ID = 'THREAD_ID'
    WF_RUN_GUID = 'WF_RUN_GUID'
    WF_SPEC_GUID = 'WF_SPEC_GUID'
    WF_SPEC_NAME = 'WF_SPEC_NAME'


class VariableAssignment(LHBaseModel):
    wf_run_variable_name: Optional[str] = None
    literal_value: Optional[Any] = None
    wf_run_metadata: Optional[WFRunMetadataEnum] = None

    json_path: Optional[str] = None
    default_value: Optional[Any] = None


class NodeType(Enum):
    TASK = 'TASK'
    EXTERNAL_EVENT = 'EXTERNAL_EVENT'
    SPAWN_THREAD = 'SPAWN_THREAD'
    WAIT_FOR_THREAD = 'WAIT_FOR_THREAD'
    SLEEP = 'SLEEP'
    THROW_EXCEPTION = 'THROW_EXCEPTION'


class LHComparisonEnum(Enum):
    LESS_THAN = 'LESS_THAN'
    GREATER_THAN = 'GREATER_THAN'
    LESS_THAN_EQ = 'LESS_THAN_EQ'
    GREATER_THAN_EQ = 'GREATER_THAN_EQ'
    EQUALS = 'EQUALS'
    NOT_EQUALS = 'NOT_EQUALS'
    IN = 'IN'
    NOT_IN = 'NOT IN'


class EdgeCondition(LHBaseModel):
    left_side: VariableAssignment
    right_side: VariableAssignment
    comparator: LHComparisonEnum


class Edge(LHBaseModel):
    source_node_name: str
    sink_node_name: str
    condition: Optional[EdgeCondition] = None


class VariableMutationOperation(Enum):
    ASSIGN = 'ASSIGN'
    ADD = 'ADD'
    SUBTRACT = 'SUBTRACT'
    MULTIPLY = 'MULTIPLY'
    DIVIDE = 'DIVIDE'
    REMOVE_IF_PRESENT = 'REMOVE_IF_PRESENT'
    REMOVE_INDEX = 'REMOVE_INDEX'
    REMOVE_KEY = 'REMOVE_KEY'


class VariableMutation(LHBaseModel):
    operation: VariableMutationOperation
    copy_directly_from_node_output: bool = False
    json_path: Optional[str] = None
    literal_value: Optional[Any] = None
    source_variable: Optional[VariableAssignment] = None


class ExceptionHandlerSpec(LHBaseModel):
    handler_thread_spec_name: str
    should_resume: bool


class Node(LHBaseModel):
    timeout_seconds: Optional[VariableAssignment] = None
    num_retries: int = 0
    node_type: NodeType = NodeType.TASK
    outgoing_edges: List[Edge] = Field(default_factory=lambda: list([]))

    variables: Optional[Mapping[str, VariableAssignment]] = None
    external_event_def_name: Optional[str] = None
    thread_wait_source_node_name: Optional[str] = None
    thread_spawn_source_node_name: Optional[str] = None

    variable_mutations: Optional[Mapping[str, VariableMutation]] = None
    task_def_name: str

    exception_to_throw: Optional[str] = None
    base_exception_handler: ExceptionHandlerSpec
    custom_exception_handlers: Mapping[str, ExceptionHandlerSpec]


class ThreadSpec(LHBaseModel):
    name: str
    entrypoint_node_name: Optional[str] = None

    variable_defs: Optional[Mapping[str, WFRunVariableDef]] = None
    interrupt_defs: Optional[Mapping[str, InterruptDef]] = None

    nodes: Mapping[str, Node] = Field(default_factory=lambda: dict({}))


class WFSpec(LHBaseModel):
    status: LHDeployStatus = LHDeployStatus.STOPPED
    desired_status: LHDeployStatus = LHDeployStatus.RUNNING

    thread_specs: Mapping[str, ThreadSpec]
    interrupt_events: Optional[List[str]] = None

    entrypoint_thread_name: str
    wf_deployer_class_name: str
    deploy_metadata: str

    def set_deploy_metadata(self, new_meta: dict):
        self.deploy_metadata = json.dumps(new_meta)
