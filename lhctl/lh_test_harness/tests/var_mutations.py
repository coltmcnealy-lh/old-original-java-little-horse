from lh_lib.schema.wf_run_schema import LHExecutionStatusEnum, WFRunSchema
from lh_test_harness.test_client import TestClient
from lh_sdk.thread_spec_builder import ThreadSpecBuilder
from lh_test_harness.test_utils import are_equal


def big_blob_task() -> dict:
    return {
        "some_blob": {
            "some_int": 1,
            "some_float": 2.5,
            "some_bool": False,
        },
        "some_list": [1, 2, 3, 4],
        "some_str": "Hello, there!",
    }


def dummy() -> str:
    return "dummy"


def return_3() -> int:
    return 3


def return_neg_2_5() -> float:
    return -2.5


def var_mutations(thread: ThreadSpecBuilder):
    my_int = thread.add_variable("my_int", int)
    my_bool = thread.add_variable("my_bool", bool)
    my_list = thread.add_variable("my_list", list)
    my_str = thread.add_variable("my_str", str)
    my_float = thread.add_variable("my_float", float)
    my_obj = thread.add_variable("my_obj", dict)

    dummy_str = thread.execute(dummy)
    my_int.assign(my_obj.jsonpath('$.my_int'))
    my_str.add(dummy_str)

    return_3_output = thread.execute(return_3)
    my_int.subtract(return_3_output)
    my_list.add(my_obj.jsonpath("$.some_thing"))

    float_mut_output = thread.execute(return_neg_2_5)
    my_float.add(float_mut_output)
    my_list.remove_idx(2)

    # Exploit the fact that we don't have JSON Schema so we can force some
    # VarSubOrzDash'es here.
    my_bool.assign(my_obj.jsonpath('$.my_bool'))
    my_obj.remove_key(1234)
    my_list.remove_if_present("asdf")


def launch_var_mutations_1(client: TestClient, wf_spec_id: str):
    wf_run_id = client.run_wf(
        wf_spec_id,
        check_var_mutations_1,
        my_obj={
            "some_thing": [1, 2, 3],
            "my_bool": True,
            "my_int": 50,
            1234: "not in the thing"
        },
        my_list=[],
        my_float=3.2
    )
    print(f"Ran wf_run_id {wf_run_id} on var_mutations case 1")


# This one checks the 
def check_var_mutations_1(wf_run: WFRunSchema):
    assert len(wf_run.thread_runs) == 1, "only one thread"
    assert wf_run.status == LHExecutionStatusEnum.COMPLETED, "wf run completed"

    vars = wf_run.thread_runs[0].variables
    assert vars is not None, "has variables"

    assert are_equal(vars['my_int'], 47), "my_int"
    assert are_equal(vars['my_bool'], True), "my_bool"
    assert are_equal(vars['my_list'], [[1, 2, 3]]), "my_list"
    assert are_equal(vars['my_str'], dummy()), "my_str"
    assert 1234 not in vars['my_obj'], "deleted 1234"  # type: ignore
    assert are_equal(vars['my_float'], 3.2 - 2.5), "my_float"
