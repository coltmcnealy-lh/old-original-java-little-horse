import json
import sys


guid = sys.argv[1]

print(json.dumps({"wfRun": guid}))

