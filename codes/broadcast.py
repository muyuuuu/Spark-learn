# 对于并行处理，Apache Spark使用共享变量。
# 当驱动程序将任务发送到集群上的执行程序时，共享变量的副本将在集群的每个节点上运行

from pyspark import SparkContext

# 广播变量用于跨所有节点保存数据副本。此变量缓存在所有计算机上
sc = SparkContext("local", "Broadcast app")
words_new = sc.broadcast(["scala", "java", "hadoop", "spark", "akka"])
# value的属性，它存储数据并用于返回广播值。
data = words_new.value
print("Stored data -> {}".format(data))
elem = words_new.value[2]
print("Printing a particular element in RDD -> {}".format(elem))

# 可以使用累加器进行求和操作或计数器
num = sc.accumulator(10)
cnt = sc.accumulator(0)
def f(x):
   global num, cnt
   num+=x
   cnt += 1
rdd = sc.parallelize([20, 30, 40, 50])
rdd.foreach(f)
# value的属性，类似于广播变量。它存储数据并用于返回累加器的值，但仅在驱动程序中可用。
final = num.value
count = cnt.value
print("Accumulated value is -> {}, cnt is -> {}".format(final, count))
