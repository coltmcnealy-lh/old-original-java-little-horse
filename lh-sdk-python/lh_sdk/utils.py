from __future__ import annotations

"""
This file is a concept prototype implementation that will show the seeds of a cool
SDK for TaskDef creation from actual code.
"""

from inspect import signature, Signature
import json
import os
from typing import TYPE_CHECKING, Any, Callable

from pydantic import BaseModel as PyThingBaseModel
from humps import camelize


if TYPE_CHECKING:
    from lh_sdk.wf_spec_schema import WFRunVariableTypeEnum


class LHBaseModel(PyThingBaseModel):
    class Config:
        alias_generator = camelize
        allow_population_by_field_name = True


def get_lh_var_type(original_type: Any) -> WFRunVariableTypeEnum:
    from lh_sdk.wf_spec_schema import WFRunVariableTypeEnum

    if original_type == str:
        return WFRunVariableTypeEnum.STRING
    elif original_type == float:
        return WFRunVariableTypeEnum.DOUBLE
    elif original_type == bool:
        return WFRunVariableTypeEnum.BOOLEAN
    elif original_type == int:
        return WFRunVariableTypeEnum.INT
    elif original_type == dict:
        return WFRunVariableTypeEnum.OBJECT
    elif original_type == list:
        return WFRunVariableTypeEnum.ARRAY
    else:
        raise RuntimeError(f"Bad class type for param: {original_type}")


def cast_all_args(func, *splat_args) -> dict:
    sig: Signature = signature(func)

    args = list(splat_args)

    out = {}
    i = 0
    for param_name in sig.parameters.keys():
        arg = args[i]
        i += 1

        param = sig.parameters[param_name]
        assert param.annotation is not None  # we know it's annotated by now

        if param.annotation in [list, dict]:
            out[param_name] = json.loads(arg)
        elif param.annotation == bool:
            out[param_name] = True if arg.lower() == 'true' else False
        else:
            assert param.annotation in [int, float, str]
            out[param_name] = param.annotation(arg)

    return out


def get_task_def_name(func: Callable):
    return func.__name__
