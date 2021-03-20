import time

from pyspark import SparkContext
from pyspark.sql import SparkSession
from operator import attrgetter
from operator import add


# 创建 spark context
sc = SparkContext("local", "PageRank")
SparkSession.builder.getOrCreate().sparkContext.setLogLevel("ERROR")


# 节点到数字
name_to_id = {}
# 节点的出度
out_degree = []
# 当前节点被哪些节点指向
in_degree = []
# 节点数量
vertex_num = 0
# 边的数量
edge_num = 0
# 名字
vtx_name = []


def get_vtx_id(name, isOut=False):
    global vertex_num, vtx_name
    if name in name_to_id:
        if isOut:
            idx = name_to_id[name]
            out_degree[idx] += 1
    else:
        vtx_name.append(name)
        name_to_id[name] = vertex_num
        vertex_num += 1
        # 同时要追加一个节点，表示当前节点的出度
        out_degree.append(1)
        # 追加一个空列表，用于存储边
        in_degree.append([])
        assert len(out_degree) == len(name_to_id)
    return name_to_id[name]


def add_edge(target_id, source_id):
    in_degree[target_id].append(source_id)


def get_data(file_name):
    global edge_num
    with open(file_name, 'r') as f:
        lines = f.readlines()
        for line in lines:
            nodes = line.split()
            target = nodes[0]
            target_id = get_vtx_id(target)
            for source in nodes[1:]:
                source_id = get_vtx_id(source, isOut=True)
                edge_num += 1
                add_edge(target_id, source_id)

get_data("link.data")
print(len(vtx_name))
assert vertex_num == len(out_degree) == len(in_degree)

# 节点的排名
rank = [1/vertex_num for i in range(vertex_num)]
rank_rdd = sc.parallelize(rank)

# 节点指向其他节点的分数
out_rank = [i / j for i, j in zip(rank, out_degree)]
out_rank_rdd = sc.parallelize(out_rank)

# 指向自己的分数
in_rank = [0 for i in range(vertex_num)]
for i in range(vertex_num):
    in_rank[i] = sum([out_rank[j] for j in in_degree[i]])
in_rank_rdd = sc.parallelize(in_rank)


# 算法参数
alpha = 0.85
max_iter_num = 100  # maximum iteration number
epsilon = 1e-5  # determines whether the iteration is over
damping_value = (1 - alpha) / vertex_num


# 开始算法
since = time.time()
for iter_num in range(max_iter_num):
    difference = rank_rdd.reduce(add)
    # 计算 rank
    rank_rdd = in_rank_rdd.map(lambda x: x * alpha + damping_value)
    difference -= rank_rdd.reduce(add)

    # in_rank 更新
    rank = rank_rdd.collect()
    out_rank = [i / j for i, j in zip(rank, out_degree)]
    out_rank_rdd = sc.parallelize(out_rank)

    for i in range(vertex_num):
        in_rank[i] = sum([out_rank[j] for j in in_degree[i]])
    in_rank_rdd = sc.parallelize(in_rank)

    print(iter_num, '---->>>>', difference)
    if difference < epsilon:
        break
print(time.time() - since)


in_rank = rank_rdd.collect()
result = [(x, y) for x, y in sorted(zip(in_rank, vtx_name))]
for item in result[-10:-1]:
    print(item)
