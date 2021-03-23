from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from operator import add


# master 为当前节点，count app 是引用的名称
sc = SparkContext("local", "count app")
# 取消日志的输出
# sc.addPyFile("Vertex.py")
SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")

# 创建RDD单词