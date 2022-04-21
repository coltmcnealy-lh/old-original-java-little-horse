from lh_sdk.thread_spec_builder import (
    ThreadSpecBuilder,
    Workflow,
)
from lh_sdk.wf_spec_schema import (
    WFRunVariableTypeEnum
)


def ask_for_name() -> str:
    return "Hey what's your name?"


def greet(name: str) -> str: 
    return f"Hello there, {name}!"


def basic_wf(thread: ThreadSpecBuilder):
    my_name_var = thread.add_variable("my_name_var", WFRunVariableTypeEnum.STRING)

    thread.execute(ask_for_name)

    the_name = thread.wait_for_event("my-name")
    my_name_var.assign(the_name)

    thread.execute(greet, my_name_var)
