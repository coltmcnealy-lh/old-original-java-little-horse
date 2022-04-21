from typing import Any, List, Mapping, Optional
from pydantic import BaseModel


class Command(BaseModel):
    variables: Optional[Mapping[str, Any]] = None


class TaskRunOutput(BaseModel):
    stdout: Optional[str] = None
    stderr: Optional[str] = None
    desired_failure_reason: Optional[str] = None


class ThreadRunOutput(BaseModel):
    tr_number: int
    variables: Optional[Mapping[str, Any]] = None
    task_runs: Optional[List[TaskRunOutput]] = None
    desired_status: Optional[str] = "COMPLETED"


class TestCase(BaseModel):
    command: Command
    output: List[ThreadRunOutput]
    timeout: float = 0.5


class TestSuite(BaseModel):
    test_cases: List[TestCase]
    wf_spec: dict
    description: Optional[str] = None
