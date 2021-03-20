# 序列化用于Apache Spark的性能调优。通过网络发送或写入磁盘或持久存储在内存中的所有数据都应序列化
# 序列化在昂贵的操作中起着重要作用

from pyspark.context import SparkContext
from pyspark.serializers import MarshalSerializer

# 使用MarshalSerializer序列化数据。
sc = SparkContext("local", "serialization app", serializer = MarshalSerializer())
print(sc.parallelize(list(range(1000))).map(lambda x: 2 * x).take(10))
sc.stop()