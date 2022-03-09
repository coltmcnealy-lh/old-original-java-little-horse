import sys
import time


if __name__ == '__main__':
    stime = int(sys.argv[1]) if len(sys.argv > 1) else 10
    time.sleep(stime)
