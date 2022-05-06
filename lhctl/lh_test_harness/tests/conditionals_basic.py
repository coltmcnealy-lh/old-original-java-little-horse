import time

from lh_lib.schema.wf_run_schema import LHExecutionStatusEnum, NodeTypeEnum, WFRunSchema
from lh_test_harness.test_client import TestClient
from lh_sdk.thread_spec_builder import ThreadSpecBuilder


def entrypoint() -> str:
    return "This is entrypoint task"


def its_under_10() -> str:
    return "Its under 10!"


def its_10_or_more() -> str:
    return "Its 10 or more!"


def feeling_lucky() -> str:
    return "You feeling lucky, punk?"


def merger_task() -> str:
    return "This task should execute on all WFRun's."


def colt() -> str:
    return "This is colt's grad year!"


# This is the workflow function that we test.
def conditionals_basic(thread: ThreadSpecBuilder):
    my_int = thread.add_variable("my_int", int)

    thread.execute(entrypoint)

    greater_than_zero = my_int.greater_than(0)

    with greater_than_zero.is_true():

        under_ten = my_int.less_than(10)
        with under_ten.is_true():
            thread.execute(its_under_10)
        with under_ten.is_false():
            thread.execute(its_10_or_more)

        is_lucky = my_int.is_in([42, 137])
        with is_lucky.is_true():
            thread.execute(feeling_lucky)

    thread.execute(merger_task)

    is_colts_number = my_int.equals(20)
    with is_colts_number.is_true():
        thread.execute(colt)


# The "input" for test case 1. It's simple; we just launch the workflow.
def launch_conditionals_basic(client: TestClient, wf_spec_id: str):
    for i in [-10, 0, 2, 10, 15, 42, 20, 137, 1888]:
        wf_run_id = client.run_wf(
            wf_spec_id,
            check_conditionals_basic,
            my_int=i,
        )
        print(f"Launched test {wf_run_id} on basic.py.")

    time.sleep(5)

# The "check" for test case 1. Verify that the tasks worked properly.
# Note that if we got this far, we already know that the records in the db match
# the WFRun itself, because that is automatically checked. So here we can just do
# some checking on the actual WFRun itself.
def check_conditionals_basic(wf_run: WFRunSchema):
    assert len(wf_run.thread_runs) == 1, "WFRun should have one thread"
    thr = wf_run.thread_runs[0]

    assert wf_run.status == LHExecutionStatusEnum.COMPLETED, "WFRun should finish"

    assert thr.variables is not None, "Thread should have variables"

    my_int = thr.variables['my_int']
    assert isinstance(my_int, int), "orig input should be int"

    tasks = [t for t in thr.task_runs if t.node_type != NodeTypeEnum.NOP]

    if my_int <= 0:
        assert len(tasks) == 2, "Should only have 2 taskruns"
        assert tasks[0].stdout == entrypoint()
        assert tasks[1].stdout == merger_task()

    elif my_int < 10:
        assert len(tasks) == 3
        assert tasks[0].stdout == entrypoint()
        assert tasks[1].stdout == its_under_10()
        assert tasks[2].stdout == merger_task()

    elif my_int == 20:
        assert len(tasks) == 4
        assert tasks[0].stdout == entrypoint()
        assert tasks[1].stdout == its_10_or_more()
        assert tasks[2].stdout == merger_task()
        assert tasks[3].stdout == colt()

    elif my_int in [42, 137]:
        assert len(tasks) == 4
        assert tasks[0].stdout == entrypoint()
        assert tasks[1].stdout == its_10_or_more()
        assert tasks[2].stdout == feeling_lucky()
        assert tasks[3].stdout == merger_task()

    else:
        assert my_int >= 10
        assert len(tasks) == 3
        assert tasks[0].stdout == entrypoint()
        assert tasks[1].stdout == its_10_or_more()
        assert tasks[2].stdout == merger_task()