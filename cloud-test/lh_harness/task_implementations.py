def dummy():
    return "This task got executed!"


def json_with_int():
    return {
        "my-int": 1234,
        "my-null-int": None,
    }


def json_with_str():
    return {
        "my-str": "asdf",
        "my-null-str": None,
    }


def json_with_bool():
    return {
        "my-false-bool": False,
        "my-true-bool": True,
        "my-null-bool": None,
    }


def json_with_double():
    return {
        "my-double": 1.01,
        "my-null-double": None,
    }


def json_with_object():
    return {
        "my-json": {
            "foo": "bar"
        },
        "my-null-json": None
    }


def json_with_array():
    return {
        "my-array": [0, 1, 2]
    }
