from __future__ import annotations

from inspect import signature, Signature
from typing import (
    Any,
    Callable,
    Mapping,
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
TYPE_TO_ENUM: Mapping[type[ACCEPTABLE_TYPES], WFRunVariableTypeEnum]= {
    str: WFRunVariableTypeEnum.STRING,
    list: WFRunVariableTypeEnum.ARRAY,
    dict: WFRunVariableTypeEnum.OBJECT,
    bool: WFRunVariableTypeEnum.BOOLEAN,
    float: WFRunVariableTypeEnum.FLOAT,
    int: WFRunVariableTypeEnum.INT,
}


class VariableJsonpath:
    def __init__(
        self,
        schema: VariableAssignmentSchema,
        node_name: str,
    ):
        self._node_name = node_name
        self._schema = schema

    @property
    def schema(self) -> VariableAssignmentSchema:
        return self._schema

    @property
    def node_name(self) -> str:
        return self._node_name


class NodeJsonpath:
    def __init__(
        self,
        schema: VariableAssignmentSchema,
        node_name: str,
    ):
        self._node_name = node_name
        self._schema = schema

    @property
    def schema(self) -> VariableAssignmentSchema:
        return self._schema

    @property
    def node_name(self) -> str:
        return self._node_name



class NodeOutput:
    def __init__(
        self,
        node_name,
        output_type: Optional[Any] = None,
        jsonpath: Optional[str] = None,
    ):
        self._node_name = node_name
        self._output_type = output_type
        self._jsonpath = jsonpath

    @property
    def output_type(self) -> Any:
        return self._output_type

    @property
    def node_name(self) -> str:
        return self._node_name

    def jsonpath(self, path: str) -> NodeOutput:
        if self._jsonpath is not None:
            raise RuntimeError(
                "Cannot double-up the jsonpath!"
            )
        return NodeOutput(
            self.node_name,
            output_type=self.output_type,
            jsonpath=path,
        )

    def get_jsonpath(self) -> Optional[str]:
        return self._jsonpath


class WFRunVariable:
    def __init__(
        self,
        name: str,
        var_type: WFRunVariableTypeEnum,
        thread: ThreadSpecBuilder,
        jsonpath: Optional[str] = None,
    ):
        self._name = name
        self._var_type = var_type
        self._thread = thread
        self._jsonpath = jsonpath

    @property
    def var_type(self) -> WFRunVariableTypeEnum:
        return self._var_type

    @property
    def name(self) -> str:
        return self._name

    def jsonpath(self, path: str) -> WFRunVariable:
        return WFRunVariable(
            self.name,
            self.var_type,
            self._thread,
            path,
        )

    def get_jsonpath(self) -> Optional[str]:
        return self._jsonpath

    def _create_mutation(
        self,
        op: VariableMutationOperation,
        target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput],
    ) -> VariableMutationSchema:
        out = VariableMutationSchema(
            operation=op,
        )

        if isinstance(target, ACCEPTABLE_TYPES):
            out.literal_value = target
        elif isinstance(target, WFRunVariable):
            out.source_variable = VariableAssignmentSchema(
                wf_run_variable_name=target.name,
                json_path=target.get_jsonpath(),
            )
        else:
            assert isinstance(target, NodeOutput)
            out.json_path = target.get_jsonpath()

        return out

    def _mutate(
        self,
        op: VariableMutationOperation,
        target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput],
    ):
        mutation = self._create_mutation(op, target)
        self._thread.mutate(self.name, mutation)


    def assign(
        self,
        target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]
    ):
        self._mutate(VariableMutationOperation.ASSIGN, target)

    def add(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.ADD, target)

    def extend(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.EXTEND, target)

    def subtract(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.SUBTRACT, target)

    def multiply(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.MULTIPLY, target)

    def divide(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.DIVIDE, target)

    def remove_if_present(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]
    ):
        self._mutate(VariableMutationOperation.REMOVE_IF_PRESENT, target)

    def remove_idx(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.REMOVE_INDEX, target)

    def remove_key(self, target: Union[ACCEPTABLE_TYPES, WFRunVariable, NodeOutput]):
        self._mutate(VariableMutationOperation.REMOVE_KEY, target)


class ThreadSpecBuilder:
    def __init__(self, name: str, wf_spec: WFSpecSchema, wf: Workflow):
        self._spec: ThreadSpecSchema = ThreadSpecSchema(name=name)
        self._spec.nodes = {}
        self._spec.edges = []
        self._last_node_name: Optional[str] = None
        self._wf_spec = wf_spec
        self._wf = wf

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
            return self.execute_task_def_name(task, **kwargs)

    def execute_task_def_name(self, td_name, **kwargs) -> NodeOutput:
        # TODO: Add ability to validate the thing by making a request to look up
        # the actual task def and seeing if it is valid
        node = NodeSchema(task_def_name=td_name)
        node.variables = {}
        for param_name in kwargs:
            arg = kwargs[param_name]
            if isinstance(arg, VariableJsonpath):
                # TODO: Validate that type is correct.
                node.variables[param_name] = arg.schema
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

            if isinstance(arg, VariableJsonpath):
                # TODO: Validate that type is correct.
                node.variables[param_name] = arg.schema
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
        var_type: Union[WFRunVariableTypeEnum, type[ACCEPTABLE_TYPES]],
        default_val: Optional[Any] = None
    ) -> WFRunVariable:

        if not isinstance(var_type, WFRunVariableTypeEnum):
            var_type = TYPE_TO_ENUM[var_type]

        var_def = WFRunVariableDefSchema(type=var_type, default_value=default_val)
        self._spec.variable_defs[name] = var_def
        return WFRunVariable(name, var_type, self)

    def mutate(self, var_name: str, mutation: VariableMutationSchema):
        assert self._last_node_name is not None, "Execute task before mutating vars!"
        node = self._spec.nodes[self._last_node_name]
        node.variable_mutations[var_name] = mutation

    def wait_for_event(self, event_name: str) -> NodeOutput:
        node = NodeSchema(external_event_def_name=event_name)
        node.node_type = NodeType.EXTERNAL_EVENT

        node_name = self._add_node(node)
        return NodeOutput(node_name)

    def _get_def_for_var(self, var_name: str) -> WFRunVariableDefSchema:
        # TODO: Traverse a tree upwards to find variables for thread-scoping stuff
        # once we add that into the SDK.
        return self._spec.variable_defs[var_name]

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
