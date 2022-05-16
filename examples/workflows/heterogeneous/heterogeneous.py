from lh_sdk.thread_spec_builder import ThreadSpecBuilder


def my_python_task() -> str: 
    return "Hello from a python task!"


def heterogeneous_wf(thread: ThreadSpecBuilder):
    # Execute a Java task and pass in an argument to the Task
    thread.execute("my-java-task", myInputVar="my-input")

    # Execute a python task that is defined above and built for this workflow in
    # the `lhctl deploy` command.
    thread.execute(my_python_task)
