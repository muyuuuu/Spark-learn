import re
import sys, time
from operator import add
from pyspark import SparkConf, SparkContext


if __name__ == '__main__':
    iterations = 10
    conf = SparkConf().setAppName('PythonPageRank')
    sc = SparkContext(conf=conf)

    data = [i for i in range(10)]
    data = sc.parallelize(data).map(lambda x: (x, 1))

    data.map(lambda x: (x[0], x[1]))
    # data.collect()
    data.foreach(print)