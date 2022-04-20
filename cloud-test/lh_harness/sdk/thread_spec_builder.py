from __future__ import annotations

from inspect import signature, Signature
from typing import (
    Any,
    Callable,
    Optional,
    Union,
)
from lh_harness.sdk.utils import get_lh_var_type, get_task_def_name

from lh_harness.sdk.wf_spec_schema import (
    EdgeSchema,
    NodeSchema,
    ThreadSpecSchema,
    VariableAssignmentSchema,
    VariableMutationOperation,
    VariableMutationSchema,
    WFRunVariableDefSchema,
    WFRunVariableTypeEnum,
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
    def __init__(self, name: str):
        self._spec: ThreadSpecSchema = ThreadSpecSchema(name=name)
        self._spec.nodes = {}
        self._spec.edges = []
        self._last_node_name: Optional[str] = None

    def execute(self, func: Callable[..., Any], *splat_args):
        sig: Signature = signature(func)

        node = NodeSchema(task_def_name=get_task_def_name(func))
        node.variables = {}

        args = [thing for thing in splat_args[0]]
        i = 0
        for param_name in sig.parameters.keys():
            arg = args[i]
            i += 1

            if isinstance(arg, VariableAssignment):
                # TODO: Validate that type is correct.
                node.variables[param_name] = arg.get_schema()
                continue

            # If we got here, then we want a literal value to be assigned.
            var_assign = VariableAssignmentSchema()
            param = sig.parameters[param_name]
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
        node_name = f'{len(self._spec.nodes)}-{node.task_def_name}'
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

        node = self._spec.nodes[self._last_node_name]
        mutation = VariableMutationSchema(operation=VariableMutationOperation.ASSIGN)

        mutation.copy_directly_from_node_output = True

    def _assign_var_var_assign(
        self,
        var: WFRunVariable,
        target: VariableAssignment
    ):
        raise NotImplementedError("Oops")
