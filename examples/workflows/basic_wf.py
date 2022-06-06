from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_lib.schema.wf_spec_schema import WFRunVariableTypeEnum


# This function is a Task! The SDK will create TaskDef and Dockerfile specs for
# it so that `lhctl deploy` will deployer a worker that runs it at the appropriate
# times.
def ask_for_name() -> str:
    return "Hey what's your name?"


# This is another Task!
def greet(name: str) -> str: 
    return f"Hello there, {name}!"


# This is a Workflow Function. It is used by the `lhctl` interpreter to automagically
# generate WFSpec, TaskDef, and Dockerfile specs needed to deploy a workflow.
def my_workflow(thread: ThreadSpecBuilder):

    # # Declare a variable. You can search WFRun's by their variable (cool!).
    # my_name_var = thread.add_variable("my_name_var", WFRunVariableTypeEnum.STRING)

    # Execute a task
    thread.execute(ask_for_name)

    # # Wait for an external event to come in. The WFRun will block here (without
    # # using any resources) indefinitely until an ExternalEvent arrives.
    # the_name = thread.wait_for_event("my-name")

    # # Assign our variable to the result from the ExternalEvent.
    # my_name_var.assign(the_name)

    # # my_name_var() takes in an argument, so let's pass in our name variable!
    # thread.execute(greet, my_name_var)
