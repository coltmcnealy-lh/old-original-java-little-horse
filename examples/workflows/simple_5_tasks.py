from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


# This is another Task!
def greet(name: str) -> str: 
    return f"Hello there, {name}!"


# This is a Workflow Function. It is used by the `lhctl` interpreter to automagically
# generate WFSpec, TaskDef, and Dockerfile specs needed to deploy a workflow.
def simple_5_tasks(thread: ThreadSpecBuilder):
    thread.execute(greet, "Obi-Wan")
    thread.execute(greet, "Anakin")
    thread.execute(greet, "Ahsoka")
    thread.execute(greet, "Yoda")
    thread.execute(greet, "Qui-Gon")
