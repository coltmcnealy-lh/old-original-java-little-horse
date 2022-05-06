from lh_sdk.thread_spec_builder import ThreadSpecBuilder


def multiply(some_number: int) -> int:
    return some_number * 3


def dummy() -> str:
    return "Hello, there!"


def happy() -> str:
    return "Yay!"


def false_dummy() -> str:
    return "false dummy"


def conditionals_else(thread: ThreadSpecBuilder):
    my_int = thread.add_variable("my_int", int)
    thread.execute(dummy)

    cond = my_int.less_than(10)
    with cond.is_true():
        my_int.assign(thread.execute(multiply, my_int))
        new_cond = my_int.greater_than(5)

        with new_cond.is_true():
            thread.execute(happy)

    with cond.is_false():
        thread.execute(false_dummy)

    thread.execute(dummy)
