# Spark 安装

和其他组件一起工作，最终编译为 java 的字节码去运行。所以需要 java、hadoop 等环境。

`usr`：unix software resource

若使用 HDFS 中的文件，需要提前启动 hadoop。

只有一台笔记本电脑，hadoop 是伪分布式：将 name node 和 data node 放到一台计算机上，实际上应该是一个 name node，多个 data node。

Spark 部署成 `local` 是单机模式，可以和 hadoop 伪分布式进行交互，可以访问 HDFS 的文件。集群部署不在是和伪分布式进行交互。

# pyspark

是一个交互式的执行环境，