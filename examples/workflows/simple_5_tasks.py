from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


# This is another Task!
def greet(name: str) -> str: 
    return f"Hello there, {name}!"

def second(name: str) -> str:
    return "another task"

def third(name: str) -> str:
    return "asdf"

def fourth(name: str) -> str:
    return "asdf"

def fifth(name: str) -> str:
    return "asdf"


# This is a Workflow Function. It is used by the `lhctl` interpreter to automagically
# generate WFSpec, TaskDef, and Dockerfile specs needed to deploy a workflow.
def simple_5_tasks(thread: ThreadSpecBuilder):
    thread.execute(greet, "Obi-Wan")
    thread.execute(second, "Anakin")
    thread.execute(third, "Ahsoka")
    thread.execute(fourth, "Yoda")
    thread.execute(fifth, "Qui-Gon")
