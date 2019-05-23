import os

from pyspark import SparkContext
from pyspark.sql import SparkSession

sc = SparkContext.getOrCreate()
sc.addPyFile(os.path.join(os.environ['SPARK_HOME'], 'jars', 'graphframes-0.7.0-spark2.4-s_2.11.jar'))
spark = SparkSession.builder.master('local').getOrCreate()

with open('data/user_list.txt') as infile:
    user_list = infile.read().split('\n')[:-1]

user_list = list(enumerate(map(int, user_list)))
vertices = spark.createDataFrame(user_list, ['id', 'name'])

def process_batch(batch_filename, batch_num):
    import graphframes
    print('processing', batch_filename)
    edges = spark.read.csv(batch_filename, header=True)
    graph = graphframes.GraphFrame(vertices, edges)
    communities = graph.labelPropagation(maxIter=5)
    comm_filename = 'data/models/comm_{}.csv'.format(batch_num)
    communities.toPandas().to_csv(comm_filename, header=True, index=False)
