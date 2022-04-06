from sdk import LHThread, LHWorkflow, LHTask # type: ignore


@LHTask
def ask_for_name():
    print("what's your name?")


@LHTask
def greeting(person_name):
    print(f"Hello there, {person_name}!")


def workflow_func(thread: LHThread):
    person_name = thread.declare_variable(str)

    thread.execute("ask-for-name-task")
    new_name = thread.wait_for_event("my-name")

    person_name.assign(new_name)
    thread.execute("greeting-task", person_name)


workflow = LHWorkflow(workflow_func)
workflow.deploy()



####################################################
# Notes
####################################################


# Using LH for the example above gives you:
# - Observability into process state
# - Persistence and reliability
# - Webhook Integration
# - Monitoring
# - Integration w/DevOps lifecycle
# all for free!





























# def analogous_python_function():
#     person_name = None
#     ask_for_name()

#     # Here it becomes complicated, you need to implement some
#     # callback logic + some backing database + some webhook
#     # in order to wait for the external event to come in.
#     # For now let's ignore that, and just use LittleHorse ;)

#     person_name = input("what's your name?")
#     greeting(person_name)