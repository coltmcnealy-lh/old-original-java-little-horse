import sys
import json
import requests


wf_run_id = sys.argv[1]
URL = "http://localhost:5000"

response = requests.get(f"{URL}/WFRun/{wf_run_id}")
response.raise_for_status()

if len(sys.argv) > 2 and sys.argv[2] == '-v':
    print(response.content.decode())
    exit(0)


wf_run = response.json()['result']


def indent(variables):
    strings = json.dumps(trun['variables'], indent=4, sort_keys=True).split('\n')

    return '\n\t'.join(strings)


print("WFRun Status:", wf_run['status'])
print("Threads:")
for trun in wf_run['threadRuns']:
    print("\tId: ", trun['id'])
    print("\tStatus: ", trun['status'])

    up_next = trun['upNext']
    if len(up_next) > 0 and up_next[0]['sinkNodeName'] == 'wait_for_mystery_person':
        print("\tWaiting for external event 'mystery_person_arrived'")

    print("\tTasks:")
    for task in trun['taskRuns']:
        print(f"\t\t{task['nodeName']}: {task['stdout']}")

    print("\tVariables: ", indent(trun['variables']))
    print()
