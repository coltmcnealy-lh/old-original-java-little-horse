import json
import sys


if __name__ == '__main__':
    l = json.loads(sys.argv[1])
    s = 0
    for thing in l:
        s += thing if type(thing) == int else len(thing)
    print(s)
