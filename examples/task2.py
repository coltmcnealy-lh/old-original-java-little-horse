import json
import sys


name = sys.argv[1]

print(json.dumps({"secondPerson": name + "___"}))
