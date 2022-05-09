from __future__ import annotations
import hashlib

from inspect import signature, Signature
from typing import (
    Any,
    Callable,
    Optional,
    Set,
    Union,
)

from lh_sdk.condition_utils import IfElseCondition
from lh_sdk.node_output import NodeOutput
from lh_sdk.utils import (
    get_lh_var_type,
    get_task_def_name,
)
from lh_lib.schema.wf_spec_schema import (
    TYPE_TO_ENUM,
    ACCEPTABLE_TYPES,
    EdgeConditionSchema,
    EdgeSchema,
    InterruptDefSchema,
    NodeSchema,
    NodeType,
    ThreadSpecSchema,
    VariableAssignmentSchema,
    VariableMutationSchema,
    WFRunVariableDefSchema,
    WFRunVariableTypeEnum,
    WFSpecSchema,
)
from lh_sdk.wf_run_variable import WFRunVariable


class ThreadSpecBuilder:
    def __init__(self, name: str, wf_spec: WFSpecSchema, wf: Workflow):
        self._spec: ThreadSpecSchema = ThreadSpecSchema(name=name)
        self._spec.nodes = {}
        self._spec.edges = []
        self._last_node_name: Optional[str] = None
        self._wf_spec = wf_spec
        self._wf = wf
        self._feeder_nodes: dict[str, Optional[EdgeConditionSchema]] = {}

        self._between_if_elses: dict[str, IfElseCondition] = {}

    def execute(
        self,
        task: Union[str, Callable[..., Any]],
        *splat_args,
        **kwargs
    ) -> NodeOutput:
        if isinstance(task, Callable):
            return self.execute_task_func(task, *splat_args)
        else:
            assert isinstance(task, str)
            return self._execute_task_def_name(task, **kwargs)

    def sleep_for(
        self,
        sleep_time: Union[int, WFRunVariable]
    ) -> None:
        node = NodeSchema(node_type=NodeType.SLEEP)
        node.timeout_seconds = self.construct_var_assign(
            sleep_time, required_type=int
        )
        self._add_node(node)

    def construct_var_assign(
        self,
        entity: Union[WFRunVariable, ACCEPTABLE_TYPES],
        required_type=None,
    ):
        if isinstance(entity, WFRunVariable):
            if required_type is not None:
                assert isinstance(required_type, type)
                assert required_type in [list, str, float, dict, bool, int]
                if get_lh_var_type(required_type) != entity.var_type:
                    raise RuntimeError("mismatched var type!")

            var_assign = VariableAssignmentSchema()
            var_assign.wf_run_variable_name = entity.name
            return var_assign

        else:
            assert isinstance(entity, ACCEPTABLE_TYPES)
            # If we got here, then we want a literal value to be assigned.
            var_assign = VariableAssignmentSchema()

            if required_type is not None:
                assert required_type == type(entity)
            var_assign.literal_value = entity
            return var_assign

    def _execute_task_def_name(self, td_name, **kwargs) -> NodeOutput:
        # TODO: Add ability to validate the thing by making a request to look up
        # the actual task def and seeing if it is valid
        node = NodeSchema(task_def_name=td_name)
        node.variables = {}

        for param_name in kwargs:
            # TODO: Maybe store JsonSchema info for variables and TaskDef's in the
            # API somewhere, and make an API call to check that. Maybe Andrew does
            # this?
            arg = kwargs[param_name]
            node.variables[param_name] = self.construct_var_assign(arg)

        node_name = self._add_node(node)
        # TODO: Add OutputType to TaskDef and lookup via api to validate it here.
        
        self._wf.mark_task_def_for_skip_build(node)
        return NodeOutput(node_name, self)

    def execute_task_func(self, func: Callable[..., Any], *splat_args) -> NodeOutput:
        sig: Signature = signature(func)

        node = NodeSchema(task_def_name=get_task_def_name(func))
        node.variables = {}

        args = list(splat_args)
        i = 0
        for param_name in sig.parameters.keys():
            param = sig.parameters[param_name]
            arg = args[i]
            i += 1
            if param.annotation is None:
                raise RuntimeError("You must annotate your parameters!")

            node.variables[param_name] = self.construct_var_assign(
                arg, required_type=param.annotation
            )

        node_name = self._add_node(node)
        if sig.return_annotation == Signature.empty:
            output_type = None
        else:
            output_type = sig.return_annotation
        return NodeOutput(node_name, self, output_type=output_type)

    def add_nop_node(self):
        self._add_node(NodeSchema(
            node_type=NodeType.NOP,
        ))

    def _add_node(self, node: NodeSchema) -> str:
        to_delete = []

        for if_else_condition_id in self._between_if_elses:
            self._between_if_elses[if_else_condition_id].handle_else_cancelled()
            to_delete.append(self._between_if_elses[if_else_condition_id]._id)

        for cond_id in to_delete:
            del self._between_if_elses[cond_id]

        if node.node_type == NodeType.TASK:
            node_human_name = node.task_def_name

        elif node.node_type == NodeType.EXTERNAL_EVENT:
            node_human_name = f'WAIT-EVENT-{node.external_event_def_name}'

        elif node.node_type == NodeType.SLEEP:
            node_human_name = 'SLEEP'

        elif node.node_type == NodeType.SPAWN_THREAD:
            node_human_name = f'SPAWN-{node.thread_spawn_thread_spec_name}'

        elif node.node_type == NodeType.WAIT_FOR_THREAD:
            node_human_name = f'WAIT-THREAD-{node.thread_wait_source_node_name}'

        elif node.node_type == NodeType.THROW_EXCEPTION:
            node_human_name = f'THROW-{node.exception_to_throw}'

        elif node.node_type == NodeType.NOP:
            node_human_name = "NOP"

        else:
            raise RuntimeError("Unimplemented nodetype")

        tag = hashlib.sha256(self._spec.name.encode()).hexdigest()[:5]
        node_name = f'{len(self._spec.nodes)}-{node_human_name}-{tag}'
        self._spec.nodes[node_name] = node

        for source_node_name in self._feeder_nodes:
            condition = self._feeder_nodes[source_node_name]
            edge = EdgeSchema(
                source_node_name=source_node_name,
                sink_node_name=node_name,
                condition=condition,
                # condition=self._next_edge_condition,
            )
            self._spec.edges.append(edge)
        self._feeder_nodes = {node_name: None}
        # self._next_edge_condition = None
        self._last_node_name = node_name
        return node_name

    def add_variable(
        self,
        name: str,
        var_type: Union[WFRunVariableTypeEnum, type[ACCEPTABLE_TYPES]],
        default_val: Optional[Any] = None
    ) -> WFRunVariable:

        if not isinstance(var_type, WFRunVariableTypeEnum):
            var_type = TYPE_TO_ENUM[var_type]

        var_def = WFRunVariableDefSchema(type=var_type, default_value=default_val)
        self._spec.variable_defs[name] = var_def
        return WFRunVariable(name, var_type, self)

    def get_parent_var(self, var_name) -> WFRunVariable:
        raise NotImplementedError()

    def _mutate(self, var_name: str, mutation: VariableMutationSchema):
        assert self._last_node_name is not None, "Execute task before mutating vars!"
        node = self._spec.nodes[self._last_node_name]
        node.variable_mutations[var_name] = mutation

    def wait_for_event(self, event_name: str) -> NodeOutput:
        node = NodeSchema(external_event_def_name=event_name)
        node.node_type = NodeType.EXTERNAL_EVENT

        node_name = self._add_node(node)
        return NodeOutput(node_name, self)

    def _get_def_for_var(self, var_name: str) -> WFRunVariableDefSchema:
        # TODO: Traverse a tree upwards to find variables for thread-scoping stuff
        # once we add that into the SDK.
        return self._spec.variable_defs[var_name]

    def handle_interrupt(
        self, event_name: str, handler: Callable[[ThreadSpecBuilder], None]
    ):
        # First, we need to create the ThreadSpec somehow.
        tspec_name = handler.__name__
        handler_builder = ThreadSpecBuilder(
            tspec_name,
            self._wf_spec,
            self._wf
        )

        self._wf_spec.thread_specs[tspec_name] = handler_builder.spec
        handler(handler_builder)

        # Ok, now the threadspec is created, so let's set the handler.
        self.spec.interrupt_defs = self.spec.interrupt_defs or {}
        self.spec.interrupt_defs[event_name] = InterruptDefSchema(
            handler_thread_name=tspec_name
        )

    @property
    def spec(self) -> ThreadSpecSchema:
        return self._spec


class Workflow:
    def __init__(
        self,
        entrypoint_function: Callable[[ThreadSpecBuilder],None],
        module_dict: dict
    ):
        self._task_def_names_to_skip: Set[str] = set({})
        self._entrypoint_func = entrypoint_function
        self._name = self._entrypoint_func.__name__

        self._spec = WFSpecSchema(
            entrypoint_thread_name="entrypoint",
            name=self._name
        )
        self._module_dict = module_dict

        self._spec.name = self._name

        self._entrypoint_builder = ThreadSpecBuilder(
            "entrypoint",
            self._spec,
            self,
        )
        self._spec.thread_specs["entrypoint"] = self._entrypoint_builder.spec
        self._entrypoint_func(self._entrypoint_builder)

    @property
    def module_dict(self) -> dict:
        return self._module_dict

    @property
    def spec(self) -> WFSpecSchema:
        return self._spec

    @property
    def name(self) -> str:
        return self._name

    @property
    def payload_str(self) -> str:
        return self.spec.json(by_alias=True)

    def mark_task_def_for_skip_build(self, node: NodeSchema):
        assert node.task_def_name is not None
        self._task_def_names_to_skip.add(node.task_def_name)

    def should_skip_build(self, node: NodeSchema) -> bool:
        return node.task_def_name in self._task_def_names_to_skip
