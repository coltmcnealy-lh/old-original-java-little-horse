from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


# This is another Task!
def greet(name: str) -> str: 
    return f"Hello there, {name}!"


# This is a Workflow Function. It is used by the `lhctl` interpreter to automagically
# generate WFSpec, TaskDef, and Dockerfile specs needed to deploy a workflow.
def speed_test(thread: ThreadSpecBuilder):

    for i in range(50):
        thread.execute(greet, "Obi-Wan")
