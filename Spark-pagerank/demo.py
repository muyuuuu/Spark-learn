from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.mllib.linalg import Vectors, SparseVector, DenseVector
from operator import add


# master 为当前节点，count app 是引用的名称
sc = SparkContext("local", "count app")
# 取消日志的输出
# sc.addPyFile("Vertex.py")
SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")
tmp = 0
def update(x):
    global tmp
    # x += tmp
    tmp += 1
    return (x, tmp)

ls = [1, 1, 1, 1, 1]
ls_rdd = sc.parallelize(ls)

ls_map = ls_rdd.map(lambda x: update(x))
ls_data = ls_map.collect()
print(ls_data)