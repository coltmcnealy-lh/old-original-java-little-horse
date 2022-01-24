import random
import json
import sys


thing = random.random()

print(thing)

if thing < 0.9:
    assert False

print(json.dumps({"asdf": "asdf"}))
