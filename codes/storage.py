# StorageLevel决定RDD是应该存储在内存中还是存储在磁盘上，或两者都存储

import pyspark
sc = pyspark.SparkContext (
   "local",
   "storagelevel app"
)
rdd1 = sc.parallelize([1,2])
rdd1.persist( pyspark.StorageLevel.MEMORY_AND_DISK_2 )
print(rdd1.getStorageLevel())