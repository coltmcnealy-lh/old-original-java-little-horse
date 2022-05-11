import random

from lh_sdk.thread_spec_builder import ThreadSpecBuilder


def unreliable_task() -> int:
    assert random.random() > 0.7
    return 5


def dummy_task() -> int:
    return 42


def exception_handler(thread: ThreadSpecBuilder):
    my_var = thread.get_parent_var("my_var")
    my_var.assign(thread.execute(dummy_task))


def my_workflow(thread: ThreadSpecBuilder):
    my_var = thread.add_variable("my_var", int, default_val=137)

    output = thread.execute(unreliable_task).catch_exception(exception_handler)
    my_var.assign(output)

    thread.execute(dummy_task)
