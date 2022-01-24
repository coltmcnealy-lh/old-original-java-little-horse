import random
import json
import sys


if random.random() > 0.8:
    assert False

print(json.dumps({"asdf": "asdf"}))
