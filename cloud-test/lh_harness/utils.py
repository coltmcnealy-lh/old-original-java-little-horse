from inspect import getsourcefile
import os


def get_file_location():
    this_file = getsourcefile(lambda: 0)
    assert this_file is not None
    dir_of_this_file = os.path.split(this_file)[0]
    return dir_of_this_file


def cleanup_case_name(case):
    if not case.endswith('.json'):
        case += '.json'

    this_file = getsourcefile(lambda: 0)
    assert this_file is not None

    dir_of_this_file = os.path.split(this_file)[0]
    test_dir = os.path.join(
        dir_of_this_file,
        "../tests/test_cases"
    )
    case = os.path.join(test_dir, os.path.split(case)[1])

    return case
