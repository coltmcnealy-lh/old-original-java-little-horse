from __future__ import annotations
import hashlib

from inspect import signature, Signature
from typing import (
    Any,
    Callable,
    Mapping,
    Optional,
    Set,
    Union,
)
import uuid
from lh_sdk.utils import get_lh_var_type, get_task_def_name

from lh_lib.schema.wf_spec_schema import (
    CONDITION_INVERSES,
    EdgeConditionSchema,
    EdgeSchema,
    InterruptDefSchema,
    LHComparisonEnum,
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
        thread: ThreadSpecBuilder,
        output_type: Optional[Any] = None,
        jsonpath: Optional[str] = None,
    ):
        self._node_name = node_name
        self._output_type = output_type
        self._jsonpath = jsonpath
        self._thread = thread

    @property
    def output_type(self) -> Any:
        return self._output_type

    @property
    def node_name(self) -> str:
        if self._thread._last_node_name != self._node_name:
            raise RuntimeError(
                "Accessing node output after other nodes executed!"
            )
        return self._node_name

    def jsonpath(self, path: str) -> NodeOutput:
        if self._thread._last_node_name != self._node_name:
            raise RuntimeError(
                "Accessing node output after other nodes executed!"
            )
        if self._jsonpath is not None:
            raise RuntimeError(
                "Cannot double-up the jsonpath!"
            )
        return NodeOutput(
            self.node_name,
            self._thread,
            output_type=self.output_type,
            jsonpath=path,
        )

    def get_jsonpath(self) -> Optional[str]:
        if self._thread._last_node_name != self._node_name:
            raise RuntimeError(
                "Accessing node output after other nodes executed!"
            )
        return self._jsonpath

class IfElseCondition:
    def __init__(
        self,
        thread: ThreadSpecBuilder,
        lhs: WFRunVariable,
        rhs: Union[ACCEPTABLE_TYPES, WFRunVariable],
        operator: LHComparisonEnum,
    ):
        self._thread = thread
        self._lhs = lhs
        self._rhs = rhs
        self._operator = operator
        self._id = uuid.uuid4().hex
        self._cancelled = False
        self._initial_feeder_node = thread._last_node_name

        self._feeder_nodes: dict[str, Optional[EdgeConditionSchema]] = {}

    @property
    def condition_schema(self) -> EdgeConditionSchema:
        return EdgeConditionSchema(
            left_side=self.left_side,
            right_side=self.right_side,
            comparator=self.operator,
        )

    @property
    def thread(self):
        return self._thread

    @property
    def left_side(self) -> VariableAssignmentSchema:
        return VariableAssignmentSchema(
            wf_run_variable_name=self._lhs.name,
            json_path=self._lhs.get_jsonpath(),
        )

    @property
    def right_side(self) -> VariableAssignmentSchema:
        if isinstance(self._rhs, WFRunVariable):
            return VariableAssignmentSchema(
                wf_run_variable_name=self._rhs.name,
                json_path=self._rhs.get_jsonpath(),
            )
        else:
            return VariableAssignmentSchema(literal_value=self._rhs)

    @property
    def reverse_condition(self) -> EdgeConditionSchema:
        condition = self.condition_schema
        new_comparator = CONDITION_INVERSES[condition.comparator]
        return EdgeConditionSchema(
            left_side=self.left_side,
            right_side=self.right_side,
            comparator=new_comparator,
        )

    def notify_thread_between_if_else(self):
        self.thread._between_if_elses[self._id] = self

    def notify_thread_else_begun(self):
        del self.thread._between_if_elses[self._id]

    # To be called if the thread starts executing crap without doing the if_else
    # first.
    def handle_else_cancelled(self):
        self._cancelled = True

    @property
    def operator(self):
        return self._operator

    def is_true(self) -> IfConditionContext:
        return IfConditionContext(self)

    def is_false(self) -> ElseConditionContext:
        if self._cancelled:
            raise RuntimeError(
                "Must call the is_false() directly after end of is_true()!"
            )
        return ElseConditionContext(self)


class IfConditionContext:
    def __init__(
        self,
        parent: IfElseCondition,
    ):
        self._parent = parent

        self._feeder_nodes: dict[str, Optional[EdgeConditionSchema]] = {}

    @property
    def parent(self):
        return self._parent

    def __enter__(self):
        new_condition = self.parent.condition_schema
        for node_name in self.parent.thread._feeder_nodes:
            if self.parent.thread._feeder_nodes[node_name] is not None:
                self.parent.thread.add_nop_node()  # Just to make things work
                break

        for node_name in self.parent.thread._feeder_nodes:
            self.parent.thread._feeder_nodes[node_name] = new_condition

        self._feeder_nodes.update(self.parent.thread._feeder_nodes)

        if self.parent.thread._last_node_name is None:
            assert len(self.parent.thread._feeder_nodes) == 0
            self.parent.thread.add_nop_node()
            assert self.parent.thread._last_node_name is not None

        self._feeder_nodes[
            self.parent.thread._last_node_name
        ] = self.parent.reverse_condition

    def __exit__(self, exc_type, exc_value, tb):
        self.parent.thread._feeder_nodes.update(self._feeder_nodes)
        self.parent.notify_thread_between_if_else()


class ElseConditionContext:
    def __init__(self, parent: IfElseCondition):
        self._parent = parent
        self._popped_node_name: Optional[str] = None

    @property
    def parent(self):
        return self._parent

    def __enter__(self):
        # The IfConditionContext has already __enter__()'ed and __exit__()'ed,
        # so we know that the parent.thread._feeder_nodes contains the last_node from
        # before the IfCondition actually entered, and it also contains the last
        # node from the if block.
        # We want to pop the last node from the if block and then re-add it later on.
        self._popped_node_name = self.parent.thread._last_node_name
        assert self._popped_node_name is not None
        assert self._popped_node_name in self.parent.thread._feeder_nodes
        del self.parent.thread._feeder_nodes[self._popped_node_name]

        # There should still be nodes in there!
        assert len(self.parent.thread._feeder_nodes) > 0

    def __exit__(self, exc_type, exc_value, tb):
        # After both the if and else blocks exit, we need the last node from
        # each block to have a null condition and both go to the next thing.
        assert self._popped_node_name is not None
        self.parent.thread._feeder_nodes[self._popped_node_name] = None


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

    @property
    def thread(self) -> ThreadSpecBuilder:
        return self._thread

    def jsonpath(self, path: str) -> WFRunVariable:
        return WFRunVariable(
            self.name,
            self.var_type,
            self._thread,
            path,
        )

    def get_jsonpath(self) -> Optional[str]:
        return self._jsonpath

    ######################################
    # Stuff for VariableMutation goes here
    ######################################

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

    #####################################
    # Stuff for conditionals follows here
    #####################################
    def less_than(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.LESS_THAN
        )

    def greater_than(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.GREATER_THAN
        )

    def greater_than_eq(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.GREATER_THAN_EQ
        )

    def less_than_eq(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.LESS_THAN_EQ
        )

    def equals(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.EQUALS
        )

    def not_equals(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.NOT_EQUALS
        )

    def contains(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        raise NotImplementedError()

    def not_contains(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        raise NotImplementedError()

    def is_in(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.IN
        )

    def is_not_in(
        self, target: Union[ACCEPTABLE_TYPES, WFRunVariable]
    ) -> IfElseCondition:
        return IfElseCondition(
            self.thread, self, target, LHComparisonEnum.NOT_IN
        )


class ThreadSpecBuilder:
    def __init__(self, name: str, wf_spec: WFSpecSchema, wf: Workflow):
        self._spec: ThreadSpecSchema = ThreadSpecSchema(name=name)
        self._spec.nodes = {}
        self._spec.edges = []
        self._last_node_name: Optional[str] = None
        self._wf_spec = wf_spec
        self._wf = wf
        self._feeder_nodes: dict[str, Optional[EdgeConditionSchema]] = {}
        # self._next_edge_condition: Optional[EdgeConditionSchema] = None

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
        sleep_time: Union[int, VariableJsonpath, WFRunVariable]
    ) -> None:
        node = NodeSchema(node_type=NodeType.SLEEP)
        node.timeout_seconds = self._construct_var_assign(
            sleep_time, required_type=int
        )
        self._add_node(node)

    def _construct_var_assign(
        self,
        entity: Union[VariableJsonpath, WFRunVariable, ACCEPTABLE_TYPES],
        required_type=None,
    ):
        if isinstance(entity, VariableJsonpath):
            # TODO: Figure out json schema so we can validate types here. Maybe
            # have Andrew do that?
            return entity.schema

        elif isinstance(entity, WFRunVariable):
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
            node.variables[param_name] = self._construct_var_assign(arg)

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

            node.variables[param_name] = self._construct_var_assign(
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

    def mutate(self, var_name: str, mutation: VariableMutationSchema):
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
