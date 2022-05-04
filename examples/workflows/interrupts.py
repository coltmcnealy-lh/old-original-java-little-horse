from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


def hello_there(name: str) -> str:
    return f"Hello there, {name}!"


# This is the interrupt handler thread!
def interrupt_handler_thread(thread: ThreadSpecBuilder):
    thread.execute(hello_there, "from the interrupt thread")


# This is a Workflow Function. It is used by the `lhctl` interpreter to automagically
# generate WFSpec, TaskDef, and Dockerfile specs needed to deploy a workflow.
def my_workflow(thread: ThreadSpecBuilder):
    # thread.handle_interrupt("some-event", interrupt_handler_thread)

    thread.execute(hello_there, "General Kenobi")
    thread.sleep_for(20)
    thread.execute(hello_there, "R2-D2")
