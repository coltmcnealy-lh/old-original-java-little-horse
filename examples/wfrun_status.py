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


class IndentContext:
    def __init__(self, printer):
        self._printer = printer

    def __enter__(self):
        self._printer._indent += 1

    def __exit__(self):
        self._printer._indent -= 1


class Printer:
    def __init__(self, indent=0):
        self._indent = indent

    def print(self, *args):
        print('\t' * self._indent, end='')
        for arg in args:
            print(arg, end='')
        print()

    def indent(self):
        self._indent += 1

    def unindent(self):
        self._indent -= 1


printer = Printer()

printer.print("WFRun Status:", wf_run['status'])
printer.print("Threads:")

printer.indent()

for trun in wf_run['threadRuns']:
    print("--->>")
    printer.print("Id: ", trun['id'])
    if trun['isInterruptThread']:
        printer.print("Interrupt thread!")
    printer.print("Status: ", trun['status'])

    printer.print("Tasks:")
    printer.indent()

    for task in trun['taskRuns']:
        if task['stdout'] is None:
            adjusted_stdout = None
        else:
            adjusted_stdout = task['stdout'].rstrip("\n")

        printer.print(f"{task['nodeName']}: {adjusted_stdout}")
    printer.unindent()

    up_next = trun['upNext']
    if len(up_next) > 0:
        next_edge = up_next[0]
        printer.print(f"Waiting on node {next_edge['sinkNodeName']}")

    printer.print("Variables:")
    printer.indent()
    for varname in trun['variables'].keys():
        printer.print(varname, ": ", json.dumps(trun['variables'][varname]))
    printer.unindent()

printer.unindent()
