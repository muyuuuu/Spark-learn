from pyspark import SparkContext, SparkConf
import time
from operator import add


# 节点到数字，数字到节点
name_to_id, id_to_name = {}, {}
# 当前节点指向了哪些边
point_edges = []
# 节点数量, 边的数量
vertex_num, edge_num = 0, 0
# 迭代次数
max_iter_num = 15
# 跳转概率
alpha = 0.85

# spark 配置
conf = SparkConf().setMaster("local[6]").setAppName("PageRank")
sc = SparkContext(conf=conf)

def get_vtx_id(name, isOut=False):
    '''
    将节点的名称映射为数字
    '''
    global vertex_num
    if name not in name_to_id:
        # 字典，key 是节点名，值是节点的编号
        name_to_id[name] = vertex_num
        # 编号到节点
        id_to_name[vertex_num] = name
        vertex_num += 1
        # 追加一个空列表，用于存储自己指向哪些边
        point_edges.append([])
    return name_to_id[name]


def add_edge(source_id, target_id):
    '''
    source_id 指向了 target_id
    '''
    point_edges[source_id].append(target_id)


def get_data(file_name):
    global edge_num
    with open(file_name, 'r') as f:
        lines = f.readlines()
        for line in lines:
            nodes = line.split()
            # 目标节点
            target = nodes[0]
            target_id = get_vtx_id(target)
            # 这些节点指向了目标节点
            for source in nodes[1:]:
                source_id = get_vtx_id(source, isOut=True)
                edge_num += 1
                add_edge(source_id, target_id)


def compute_contribs(x):
    '''
    当前节点，以及分数
    '''
    urls, rank = x[1][0], x[1][1]
    num_urls = len(urls)
    for url in urls:
        yield (url, rank / num_urls)


if __name__ == "__main__":
    
    get_data("link.data")
    print("Loaded data")

    print("Vertex number is {}, Edge number is {}".format(vertex_num, edge_num))
    assert vertex_num == len(point_edges)

    # 临时变量，便于映射
    nodes = [i for i in range(vertex_num)]
    # 生成 RDD，RDD的结构是 <id, [urls]>, id 是节点编号，urls 是自己指向了哪些节点，一个列表
    links = sc.parallelize(nodes).map(lambda x: (x, point_edges[x])).cache()
    # 初始化 rank 值
    ranks = sc.parallelize(nodes).map(lambda x: (x, 1/vertex_num)).cache()
    print("Initialize RDD of links and ranks")

    jump_value = (1 - alpha) / vertex_num

    since = time.time()
    for i in range(max_iter_num):
        # join 把节点编号相同的放在一起，[urls, rank]
        # flatMap 计算每个节点所带有的分数，返回 <url_id, rank>
        contribs = links.join(ranks).flatMap(lambda x: compute_contribs(x))
        # 将 url_id 相同的聚合在一起，也就是被指向的节点分数求和，并更新 rank 值
        ranks = contribs.reduceByKey(add).mapValues(lambda rank: rank * alpha + jump_value)
    print(time.time() - since)
    for (link, rank) in ranks.sortBy(lambda x: x[1], False).take(5):
        print("{}, {}".format(id_to_name[link], rank))
    print(time.time() - since)