import random

from lh_lib.schema.wf_run_schema import LHExecutionStatusEnum, WFRunSchema
from lh_test_harness.test_client import TestClient
from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_test_harness.test_utils import are_equal


def dummy_task() -> str:
    return "Hello, there!"


def unreliable_task() -> str:
    assert random.random() > 0.5
    return dummy_task()


def retries_task(thread: ThreadSpecBuilder):
    thread.execute(dummy_task)
    thread.execute(unreliable_task).with_retries(2)
    thread.execute(dummy_task)


def launch_retries_task(client: TestClient, wf_spec_id: str):
    for _ in range(20):
        wf_run_id = client.run_wf(wf_spec_id, check_retries_task)
        print(f"Launched test {wf_run_id} on basic.py.")


def check_retries_task(wf_run: WFRunSchema):
    assert len(wf_run.thread_runs) == 1
    thr = wf_run.thread_runs[0]

    assert thr.task_runs[0].stdout == dummy_task()

    task = thr.task_runs[1]
    task_idx = 1
    assert task.number == 1, "Second task should be number 1"

    while True:
        assert task.number == 1, "Went to next task prematurely!"
        task_idx += 1
        if task.status == LHExecutionStatusEnum.COMPLETED:
            assert task.stdout == dummy_task()
            break
        else:
            assert task.status == LHExecutionStatusEnum.HALTED
            assert task.stderr is not None, "Should have error reporting"
            assert 'AssertionError' in task.stderr, "Should have something relevant"

        if task.attempt_number == 2:
            # Then this is the last one
            assert wf_run.status == LHExecutionStatusEnum.HALTED
            assert thr.status == LHExecutionStatusEnum.HALTED
            return

        task = thr.task_runs[task_idx]

    assert wf_run.status == LHExecutionStatusEnum.COMPLETED
    assert thr.status == LHExecutionStatusEnum.COMPLETED
    task = thr.task_runs[task_idx]
    assert task.stdout == dummy_task()