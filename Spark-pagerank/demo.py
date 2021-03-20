from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add
from Vertex import Vertex


# master 为当前节点，count app 是引用的名称
sc = SparkContext("local", "count app")
# 取消日志的输出
sc.addPyFile("Vertex.py")
SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")

# 创建RDD单词
words = sc.parallelize ([Vertex('aa') for i in range(10)])

words_map = words.map(lambda x: x.rank / 10)
a = words_map.collect()
for i in a:
   print(i.rank)