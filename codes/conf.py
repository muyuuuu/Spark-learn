# 可以使用SparkConf对象设置不同的参数，它们的参数将优先于系统属性。

from pyspark import SparkConf, SparkContext
conf = SparkConf().setAppName("PySpark App").setMaster("spark://master:7077")
sc = SparkContext(conf=conf)