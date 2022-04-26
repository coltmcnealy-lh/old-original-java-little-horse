import argparse
import importlib
import json

from lh_sdk.utils import cast_all_args


parser = argparse.ArgumentParser('lhctl_execute')
parser.add_argument(
    "module_name",
    help="Name of module in which task function resides.",
)
parser.add_argument(
    "func_name",
    help="Name of function to execute for the task.",
)

args, unknown = parser.parse_known_args()

module = importlib.import_module(args.module_name)
func = module.__dict__[args.func_name]

new_args = cast_all_args(func, *unknown)

result = func(*new_args)

if isinstance(result, list) or isinstance(result, dict):
    result = json.dumps(result)

# This is a pretty silly way of serializing, but whatevs
print(result, end='')
