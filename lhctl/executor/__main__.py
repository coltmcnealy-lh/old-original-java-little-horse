import argparse
import importlib
import json

from lh_sdk.utils import cast_all_args, parse_task_def_name


parser = argparse.ArgumentParser('lhctl_execute')
parser.add_argument(
    "task_def_name",
    help="TaskDef name. Formated as module_name.replace('.','-') + '-' + func_name.",
)

args, unknown = parser.parse_known_args()

module_name, func_name = parse_task_def_name(args.task_def_name)
module = importlib.import_module(module_name)
func = module.__dict__[func_name]

new_args = cast_all_args(func, *unknown)

result = func(*new_args)

if isinstance(result, list) or isinstance(result, dict):
    result = json.dumps(result)

# This is a pretty silly way of serializing, but whatevs
print(result, end='')
