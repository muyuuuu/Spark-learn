# Arch 安装 Spark

## AUR 安装

- 自己安装过于繁琐
- `yay -S apache-spark` 直接安装 Spark 及相关依赖（Scala，hadoop，python等），需要科学上网。
- 写入环境变量：`export PATH=$PATH:/opt/apache-spark/bin`

## docker 安装

- `docker pull bitnami/spark:latest`
- 阿里云镜像加速地址：https://cr.console.aliyun.com/cn-hangzhou/instances/mirrors

# 学习记录

- firstapp.py，第一个计数程序，shell 执行 `spark-submit firstapp.py`
- 
