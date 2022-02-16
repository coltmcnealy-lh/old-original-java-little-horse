import os
import requests
import subprocess
import sys
import threading


URL = 'http://localhost:30000'

wf_spec = requests.get(f"{URL}/WFSpecAlias/name/{sys.argv[1]}")
wf_spec = wf_spec.json()['result']

topics = [wf_spec['eventTopic']]
pod = subprocess.check_output("kubectl get po -l app=kafka-broker".split()).decode().split('\n')[1].split()[0]


def get_task_queue_name(node):
    resp = requests.get(f"{URL}/TaskDef/{node['taskDefId']}")
    return resp.json()['result']['taskQueueName']
    
    

for thread_name in wf_spec['threadSpecs'].keys():
    thread = wf_spec['threadSpecs'][thread_name]

    for node_name in thread['nodes'].keys():
        node = thread['nodes'][node_name]
        if node['taskDefName'] is None:
            continue

        topics.append(get_task_queue_name(node))


def watch_topic(topic):
    print("Watching topic:", topic)
    command = f"kubectl exec -it {pod} -- /bin/kafka-console-consumer --bootstrap-server kafka-broker:9092 --topic {topic} --from-beginning"
    os.system(command)


for topic in set(topics):
    t = threading.Thread(target=watch_topic, args=(topic,))
    t.start()
