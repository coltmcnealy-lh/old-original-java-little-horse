import os
import requests
import subprocess
import sys
import threading


URL = 'http://localhost:30000'

pod = subprocess.check_output("kubectl get po -l app=kafka-broker".split()).decode().split('\n')[1].split()[0]


def watch_topic(topic):
    print("Watching topic:", topic)
    command = f"kubectl exec -it {pod} -- /bin/kafka-console-consumer --bootstrap-server kafka-broker:9092 --topic {topic} --from-beginning"
    print(command, flush=True)
    os.system(command)


t = threading.Thread(target=watch_topic, args=(sys.argv[1],))
t.start()
