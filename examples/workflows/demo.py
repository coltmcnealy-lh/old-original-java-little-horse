from lh_sdk.thread_spec_builder import ThreadSpecBuilder


def what_is_trade_value() -> str:
    return "What's the value of the trade?"


def ask_for_approval(value: int) -> str:
    return f"Do you REALLY want to make a trade worth {value}?"


def execute_trade() -> str:
    return "Executing trade now!"


def cancel_trade() -> str:
    return "Cancelling trade!"


def demo(thread: ThreadSpecBuilder):
    trade_value = thread.add_variable("trade_value_var", int)
    approved = thread.add_variable("approved", bool)

    thread.execute(what_is_trade_value)

    trade_value.assign(
        thread.wait_for_event("trade-value")
    )

    needs_approval = trade_value.greater_than(1000)

    with needs_approval.is_true():
        thread.execute(ask_for_approval, trade_value)
        approved.assign(thread.wait_for_event("approval"))

        is_approved = approved.equals(True)
        with is_approved.is_true():
            thread.execute(execute_trade)
        with is_approved.is_false():
            thread.execute(cancel_trade)

    with needs_approval.is_false():
        thread.execute(execute_trade)


