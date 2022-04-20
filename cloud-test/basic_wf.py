from lh_harness.sdk.thread_spec_builder import (
    NodeOutput,
    ThreadSpecBuilder,
    WFRunVariable,
)
from lh_harness.sdk.wf_spec_schema import (
    WFRunVariableTypeEnum
)


def ask_for_name() -> str:
    return "Hey what's your name?"


def greet(name: str) -> str: 
    return f"Hello there, {name}!"


def workflow(thread: ThreadSpecBuilder):
    myvar = thread.add_variable("my_var", WFRunVariableTypeEnum.STRING)

    output = thread.execute(ask_for_name)
    myvar.assign(output)

    thread.execute(greet, myvar)


if __name__ == '__main__':
    builder = ThreadSpecBuilder("my-thread")
    workflow(builder)
    print(builder._spec.json(by_alias=True))
