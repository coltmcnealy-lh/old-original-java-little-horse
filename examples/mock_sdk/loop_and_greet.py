from sdk_not_implemented_yet import LHTask, LHWorkflow, LHThread, LHClient, LHEvent # type: ignore


def basic_greeting(person_name: str):
    print("What's cooking, " + person_name + '?')


def formal_greeting(person_name: str):
    print("Good afternoon, " + person_name + '?')


basic_greeting_task = LHTask(basic_greeting)
formal_greeting_task = LHTask(formal_greeting)

set_name_event = LHEvent("new_name")


def interrupt_handler(thread: LHThread, new_name):
    thread.assign("person_name", new_name)


def workflow_func(thread: LHThread):
    person_name = thread.declare("person_name", str)

    thread.handle_interrupt(
        set_name_event,
        interrupt_handler,
    )

    with thread.do_while(True):
        condition = thread.condition(person_name.is_in(['Raj', 'Scott']))

        with condition.do_if_true():
            thread.execute(formal_greeting_task(person_name))
        with condition.do_else():
            thread.execute(basic_greeting_task(person_name))

        thread.sleep_seconds(10)


if __name__ == '__main__':

    # Create an API client with appropriate configuration
    client = LHClient(
        api_url="http://little-horse-deployment.altimetrik.com",
        wf_deployer='little.horse.lib.deployers.examples.k8s.K8sWorkflowDeployer',
        task_deployer='little.horse.lib.deployers.examples.k8s.K8sTaskDeployer',
    )

    # Deploy the task workers
    client.deploy_task(formal_greeting_task)
    client.deploy_task(basic_greeting_task)

    # Deploy the workflow
    workflow = LHWorkflow(workflow_func, name="my-workflow")
    client.deploy_workflow(workflow)

    # At this point, you can just run the workflow via API calls embedded in
    # an application.
    workflow_run_id = client.run_workflow("my-workflow")
    print(workflow_run_id)