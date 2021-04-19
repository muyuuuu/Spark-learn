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

- 代码执行： `spark-submit --master local main.py`
- spark-submit 是在spark安装目录中bin目录下的一个shell脚本文件，用于在集群中启动应用程序
- 只保留`WARN`级别及其以上的日志输出：`SparkSession.builder.getOrCreate().sparkContext.setLogLevel("WARN")`

## pagerank 记录

1. 超大规模邻接矩阵会内存爆炸
2. 邻接矩阵法失败，map 无法更新，只能获取要操作的属性，不保留全部对象。map 操作对象的 `rank` 属性时，操作结果只保留 rank，不保留对象。
```py
class Vertex:
    def __init__(self, name):
        self.name = name
        self.from_nodes = []
        self.in_degree = 0
        self.to_nodes = []
        self.out_degree = 0
        self.rank = 0
        self.out_rank = 0
```

# PPT 内容

传统算法实现，是一个迭代过程。
1. 初始化分数，按照节点的出度更新分数
2. 按照公式更新每个节点的分数。具体方式为：查找哪些节点指向当前节点，对这些节点的分数求和作为当前节点的分数，遍历所有节点
3. 更新每个点的分数后，除以每个节点的出度，进行下次迭代

耗时 3 秒。

Spark 版本实现。可以借助 `map` 等操作快速完成计算。好的 RDD 和好的 `map` 操作能事半功倍，但这需要经验。
1. 创建 `links` RDD，含义为当前节点指向了哪些链接。一个 RDD 元素的结构为 <id, [urls]>
2. 创建 `rank` RDD，含义为当前节点的分数
3. 开始迭代
4. join 操作，把 links 和 rank 按照 key 相同连接在一起，形成的新的 RDD，元素结构为 <id, [urls], rank>
5. 对新的 RDD 进行 flatmap 操作，将节点的分数除以出度并打散，返回目标元素的结构为 <target_id, rank>，即当前节点指向一个节点，那个节点的分数，flatmap 的操作一个 key 可能对应多个元素。返回 RDD 的结构为 <id, rank>，当前 RDD 命名为 temp
6. 首先对 temp 进行 reduceByKey(func) 操作，汇集 key 相同的元素，进行 func 函数指定的操作，这里 func 为 add。而后进行 mapValues 操作，只操作值，仍保留键。值的操作为 `rank * alpha + jump_value`，表示沿着链接访问当前页面的分数加上跳转到当前页面的分数。结果 RDD 复制给 `rank` RDD。进行下一轮迭代
7. 每一行代码都有详细的注释

```python
def compute_contribs(x):
    urls, rank = x[1][0], x[1][1]
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)

for i in range(max_iter_num):
    contribs = links.join(ranks).flatMap(lambda x: compute_contribs(x))
    ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * alpha + jump_value)
```

1. 6个 CPU，6个分区，0.55 秒建立好依赖关系，36秒计算出结果
2. 6个 CPU，6个分区，21 秒建立好依赖关系，13秒计算出结果
3. 