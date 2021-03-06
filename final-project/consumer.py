import os
import json
from operator import itemgetter

import findspark
findspark.init()

import kafka
from pyspark import SparkContext
from pyspark.sql import SparkSession

import pda

topic = 'twitter-network'
broker = 'localhost:9092'
json_deserializer = lambda x: json.loads(x.decode())

consumer = kafka.KafkaConsumer(
    topic,
    bootstrap_servers=[broker],
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='big-data',
    value_deserializer=json_deserializer)

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.master('local').getOrCreate()


def save_batch(batch, batch_num):
    edges = list(map(lambda x: (x['src'], x['dst']), batch))
    df = spark.createDataFrame(edges, ['src', 'dst'])
    batch_filename = 'data/batch/batch_{}.csv'.format(batch_num)
    df.toPandas().to_csv(batch_filename, header=True, index=False)
    batch.clear()
    return batch_filename


batch_size = 100000
batch_count = 0
batch = list()

for message in consumer:
    value = message.value
    print('{}: {}'.format(len(batch), value))
    batch.append(message.value)

    if len(batch) == batch_size:
        batch_filename = save_batch(batch, batch_count)
        pda.process_batch(batch_filename, batch_count)
        batch_count += 1
