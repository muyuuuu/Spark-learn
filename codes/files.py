from pyspark import SparkContext
from pyspark import SparkFiles
finddistance = "/home/lanling/github/Spark-learn/codes/files.py"
filename = "files.py"
sc = SparkContext("local", "SparkFile App")
sc.addFile(finddistance)
# 获取工作者的路径, 指定通过SparkContext.addFile（）添加的文件的路径。
print("Absolute Path -> {}".format(SparkFiles.get(filename)))