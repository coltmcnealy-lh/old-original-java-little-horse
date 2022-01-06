import json
import time

if __name__ == '__main__':
    print(json.dumps({
            'theForce': "with us",
            'when': time.time()
        }
    ))