from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


def parent_task(input_str: str) -> str:
    return f'Called parent_task() with input "{input_str}"!'


def child_task(input_str: str) -> str:
    return f'Called child_task() with input "{input_str}"'


def subthread(thread: ThreadSpecBuilder):
    parent_var = thread.get_parent_var("parent_var")
    thread.execute(child_task, parent_var)
    parent_var.assign("child value")


def my_workflow(thread: ThreadSpecBuilder):
    parent_var = thread.add_variable("parent_var", WFRunVariableTypeEnum.STRING)

    thread.execute(parent_task, parent_var)

    child = thread.spawn_thread(subthread)

    thread.wait_for_thread(child)

