from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import add


# master 为当前节点，count app 是引用的名称
sc = SparkContext("local", "count app")
# 取消日志的输出
SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")

# 创建RDD单词
words = sc.parallelize (
   ["scala",
   "java",
   "hadoop",
   "spark",
   "akka",
   "spark vs hadoop",
   "pyspark",
   "pyspark and spark"]
)

# 计数,返回RDD中的元素数。
counts = words.count()
print("Number of elements in RDD -> {}".format(counts))

# 搜集, 返回RDD中的所有元素。
coll = words.collect()
print("Elements in RDD -> {}".format(coll))

# foreach, 打印RDD中满足要求的元素。
def f(x):
    if x[0] == 's':
        print(x)
fore = words.foreach(f)

# 过滤，返回一个包含元素的新RDD
words_filter = words.filter(lambda x: 'spark' in x)
filtered = words_filter.collect()
print("Fitered RDD -> {}".format(filtered))

# map，应用于RDD中的每个元素来返回新的RDD
words_map = words.map(lambda x: (x, 1))
mapping = words_map.collect()
print("Key value pair -> {}".format(mapping))

# reduce 
nums = sc.parallelize([1, 2, 3, 4, 5])
adding = nums.reduce(add)
print("Adding all the elements -> {}".format(adding))
print(type(adding))

# join
x = sc.parallelize([("spark", 1), ("hadoop", 4)])
y = sc.parallelize([("spark", 2), ("hadoop", 5)])
joined = x.join(y)
final = joined.collect()
print("Join RDD -> {}".format(final))

# cache
words.cache()
caching = words.persist().is_cached
print("Words got chached -> {}".format(caching))