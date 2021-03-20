# Arch 安装 Spark

## AUR 安装

- 自己安装过于繁琐
- `yay -S apache-spark` 直接[安装](https://wiki.archlinux.org/index.php/Apache_Spark) Spark 及相关依赖（Scala，hadoop，python等），需要科学上网。
- 写入环境变量：`export PATH=$PATH:/opt/apache-spark/bin`

## docker 安装

- `docker pull bitnami/spark:latest`
- 阿里云镜像加速地址：https://cr.console.aliyun.com/cn-hangzhou/instances/mirrors

# 学习记录

## 代码执行

- 代码执行： `spark-submit filename.py`
- spark-submit 是在spark安装目录中bin目录下的一个shell脚本文件，用于在集群中启动应用程序
- 只保留`WARN`级别及其以上的日志输出：`SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")`
