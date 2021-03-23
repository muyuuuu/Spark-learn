from operator import add
import numpy as np
from pyspark import SparkConf, SparkContext


# 配置文件
conf = SparkConf().setMaster("local").setAppName("PageRank")
sc = SparkContext(conf=conf)

# 加载文件为 RDD
# links_data_rdd = sc.textFile("link.data")

# 获取最初的概率转移矩阵
a = np.zeros((10, 10))
for num in range(60):
    for row, col in zip(np.random.randint(0, 10, 1), np.random.randint(0, 10, 1)):
        a[row, col] = 1.0

# 初始化分数
pagerank = sc.parallelize([1 / 10 for i in range(10)])

# 修正节点没有出度的问题
revise = np.zeros((10, 10))
zero = np.sum(a, axis=0)
for i in zero:
    if i == 0:
        revise[:, i] = 1 / 10

a += revise

# 一个页面指向两个页面，那么分数要平分
a = a / np.sum(a, axis=0)

alpha = 0.85
jump_value = 0.15 / 10
difference = 1e-4
max_num = 100
tmp_pagerank = 0

for i in range(max_num):
    # 减少计算量
    if i % 9 == 0:
        tmp = pagerank.reduce(add)
        if np.sum(tmp - tmp_pagerank) < difference:
            break
        else:
            tmp_pagerank = tmp
    pagerank = pagerank.map(lambda x: (alpha * a + jump_value) * x)

print(pagerank.collect())