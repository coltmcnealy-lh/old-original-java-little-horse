from __future__ import annotations

from inspect import signature, Signature
from typing import (
    Any,
    Callable,
    NoReturn,
    Optional,
    Set,
    Union,
)
from lh_sdk.utils import get_lh_var_type, get_task_def_name

from lh_lib.schema.wf_spec_schema import (
    EdgeSchema,
    NodeSchema,
    NodeType,
    ThreadSpecSchema,
    VariableAssignmentSchema,
    VariableMutationOperation,
    VariableMutationSchema,
    WFRunVariableDefSchema,
    WFRunVariableTypeEnum,
    WFSpecSchema,
)


ACCEPTABLE_TYPES = Union[str, list, dict, int, bool, float]


class VariableAssignment:
    def __init__(self):
        pass

    def get_schema(self) -> VariableAssignmentSchema:
        raise NotImplementedError()


class NodeOutput:
    def __init__(self, node_name, output_type: Optional[Any] = None):
        self._node_name = node_name
        self._output_type = output_type

    @property
    def output_type(self) -> Any:
        return self._output_type

    @property
    def node_name(self) -> str:
        return self._node_name


class WFRunVariable:
    def __init__(
        self,
        name: str,
        var_type: WFRunVariableTypeEnum,
        thread: ThreadSpecBuilder
    ):
        self._name = name
        self._var_type = var_type
        self._thread = thread

    @property
    def var_type(self) -> WFRunVariableTypeEnum:
        return self._var_type

    @property
    def name(self) -> str:
        return self._name

    def assign(self, target: Union[ACCEPTABLE_TYPES, VariableAssignment, NodeOutput]):
        self._thread.assign_var(
            self,
            target,
        )

class ThreadSpecBuilder:
    def __init__(self, name: str, wf_spec: WFSpecSchema, wf: Workflow):
        self._spec: ThreadSpecSchema = ThreadSpecSchema(name=name)
        self._spec.nodes = {}
        self._spec.edges = []
        self._last_node_name: Optional[str] = None
        self._wf_spec = wf_spec
        self._wf = wf

    def execute(self, task: Union[str, Callable[..., Any]], *splat_args, **kwargs):
        if isinstance(task, Callable):
            self.execute_task_func(task, *splat_args)
        else:
            assert isinstance(task, str)
            self.execute_task_def_name(task, **kwargs)

    def execute_task_def_name(self, td_name, **kwargs):
        # TODO: Add ability to validate the thing by making a request to look up
        # the actual task def and seeing if it is valid
        node = NodeSchema(task_def_name=td_name)
        node.variables = {}
        for param_name in kwargs:
            arg = kwargs[param_name]
            if isinstance(arg, VariableAssignment):
                # TODO: Validate that type is correct.
                node.variables[param_name] = arg.get_schema()
                continue

            elif isinstance(arg, WFRunVariable):
                var_assign = VariableAssignmentSchema()
                var_assign.wf_run_variable_name = arg.name
                node.variables[param_name] = var_assign
                continue

            else:
                # If we got here, then we want a literal value to be assigned.
                var_assign = VariableAssignmentSchema()
                var_assign.literal_value = arg
                node.variables[param_name] = var_assign

        node_name = self._add_node(node)
        # TODO: Add OutputType to TaskDef and lookup via api to validate it here.
        
        self._wf.mark_task_def_for_skip_build(node)
        return NodeOutput(node_name)

    def execute_task_func(self, func: Callable[..., Any], *splat_args):
        sig: Signature = signature(func)

        node = NodeSchema(task_def_name=get_task_def_name(func))
        node.variables = {}

        args = list(splat_args)
        i = 0
        for param_name in sig.parameters.keys():
            param = sig.parameters[param_name]
            arg = args[i]
            i += 1

            if isinstance(arg, VariableAssignment):
                # TODO: Validate that type is correct.
                node.variables[param_name] = arg.get_schema()
                continue

            elif isinstance(arg, WFRunVariable):
                if get_lh_var_type(param.annotation) != arg.var_type:
                    raise RuntimeError("mismatched var type!")

                var_assign = VariableAssignmentSchema()
                var_assign.wf_run_variable_name = arg.name
                node.variables[param_name] = var_assign
                continue

            else:
                # If we got here, then we want a literal value to be assigned.
                var_assign = VariableAssignmentSchema()
                if param.annotation is None:
                    raise RuntimeError("You must annotate your parameters!")

                assert param.annotation == type(arg)
                var_assign.literal_value = arg
                node.variables[param_name] = var_assign

        node_name = self._add_node(node)

        if sig.return_annotation == Signature.empty:
            output_type = None
        else:
            output_type = sig.return_annotation

        return NodeOutput(node_name, output_type=output_type)

    def _add_node(self, node: NodeSchema) -> str:
        if node.node_type == NodeType.TASK:
            node_human_name = node.task_def_name

        elif node.node_type == NodeType.EXTERNAL_EVENT:
            node_human_name = f'wait-event-{node.external_event_def_name}'

        elif node.node_type == NodeType.SLEEP:
            node_human_name = 'SLEEP'

        elif node.node_type == NodeType.SPAWN_THREAD:
            node_human_name = f'spawn-{node.thread_spawn_thread_spec_name}'

        elif node.node_type == NodeType.WAIT_FOR_THREAD:
            node_human_name = f'wait-thread-{node.thread_wait_source_node_name}'

        elif node.node_type == NodeType.THROW_EXCEPTION:
            node_human_name = f'throw-{node.exception_to_throw}'

        else:
            raise RuntimeError("Unimplemented nodetype")

        node_name = f'{len(self._spec.nodes)}-{node_human_name}'
        self._spec.nodes[node_name] = node

        if self._last_node_name is not None:
            edge = EdgeSchema(
                source_node_name=self._last_node_name,
                sink_node_name=node_name
            )
            self._spec.edges.append(edge)
        self._last_node_name = node_name
        return node_name

    def add_variable(
        self,
        name: str,
        var_type: WFRunVariableTypeEnum,
        default_val: Optional[Any] = None
    ) -> WFRunVariable:

        var_def = WFRunVariableDefSchema(type=var_type, default_value=default_val)
        self._spec.variable_defs[name] = var_def
        return WFRunVariable(name, var_type, self)

    def assign_var(
        self,
        var: WFRunVariable,
        target: Union[ACCEPTABLE_TYPES, VariableAssignment, NodeOutput],
    ):
        if isinstance(target, VariableAssignment):
            self._assign_var_var_assign(var, target)
        elif isinstance(target, NodeOutput):
            self._assign_var_node_output(var, target)
        else:
            self._assign_var_literal(var, target)

    def wait_for_event(self, event_name: str) -> NodeOutput:
        node = NodeSchema(external_event_def_name=event_name)
        node.node_type = NodeType.EXTERNAL_EVENT

        node_name = self._add_node(node)
        return NodeOutput(node_name)

    def _get_def_for_var(self, var_name: str) -> WFRunVariableDefSchema:
        # TODO: Traverse a tree upwards to find variables for thread-scoping stuff
        # once we add that into the SDK.
        return self._spec.variable_defs[var_name]

    def _assign_var_literal(
        self,
        var: WFRunVariable,
        target: ACCEPTABLE_TYPES
    ):
        if self._last_node_name is None:
            raise RuntimeError("Tried to assign var before executing stuff!")

        node = self._spec.nodes[self._last_node_name]
        mutation = VariableMutationSchema(operation=VariableMutationOperation.ASSIGN)

        # Need to validate that we aren't breaking any type ruling here.
        needed_type = self._get_def_for_var(var.name).type
        if get_lh_var_type(target) != needed_type:
            raise RuntimeError(
                f"Tried to assign {var.name} to wrong type, needed {needed_type}!"
            )

        mutation.literal_value = target
        node.variable_mutations[var.name] = mutation

    def _assign_var_node_output(
        self,
        var: WFRunVariable,
        target: NodeOutput,
    ):
        if self._last_node_name is None:
            raise RuntimeError("Tried to assign var before executing stuff!")

        if target.node_name != self._last_node_name:
            raise RuntimeError("Tried to assign variable out of order.")

        node = self._spec.nodes[self._last_node_name]
        mutation = VariableMutationSchema(operation=VariableMutationOperation.ASSIGN)
        node.variable_mutations[var.name] = mutation

    def _assign_var_var_assign(
        self,
        var: WFRunVariable,
        target: VariableAssignment
    ):
        raise NotImplementedError("Oops")

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
