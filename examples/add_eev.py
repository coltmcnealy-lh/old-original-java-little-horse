import json
from pprint import pprint as pp
import sys
import requests


URL = "http://localhost:5000"

if __name__ == '__main__':
    with open(sys.argv[1], 'r') as f:
        data = json.loads(f.read())

    response = requests.post(f"{URL}/ExternalEventDef", json=data)
    print(response.json())
