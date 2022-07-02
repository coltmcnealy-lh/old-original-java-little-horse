from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


def task1() -> str:
    return "Hey what's your name?"

def task2() -> str: 
    return "Hello there!"


def test_wf(thread: ThreadSpecBuilder):
    # Execute a task
    for i in range(25):
        thread.execute(task1)
        thread.execute(task2)
