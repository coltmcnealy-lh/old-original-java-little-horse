import random
import sys


if __name__ == '__main__':
    if len(sys.argv) > 1:
        lower = int(sys.argv[1])
        upper = int(sys.argv[2])
    else:
        lower = 0
        upper = 10

    rval = random.random()
    print(lower + int((upper - lower) * rval))
