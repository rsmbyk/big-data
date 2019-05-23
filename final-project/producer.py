import json
import time

import kafka

json_serializer = lambda x: json.dumps(x).encode()

producer = kafka.KafkaProducer(value_serializer=json_serializer)
graph_cb_file = 'data/graph_cb.txt'
topic = 'twitter-network'

with open(graph_cb_file) as infile:
    graph_cb = infile.read().split('\n')[:-1]

for line in graph_cb:
    src, dst, t = line.split()
    value = {'src': src, 'dst': dst}
    producer.send(topic, value=value)
    time.sleep(0.1)
