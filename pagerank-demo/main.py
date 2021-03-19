from operator import attrgetter
import time


class Vertex(object):
    def __init__(self):
        self.in_edge = []
        self.out_edge = []
        self.score = 0.0
        self.out_score = 0.0
        self.name = ""


class Edge(object):
    def __init__(self, start, end):
        self.start_id = start
        self.end_id = end


def addVertex(vertex_name, vtx_to_num, num_to_vtx):
    '''
    将域名映射为 ID
    '''
    res_id = 0
    if vertex_name in vtx_to_num:
        return vtx_to_num[vertex_name]
    else:
        res_id = len(vtx_to_num)
        vtx_to_num[vertex_name] = res_id
        num_to_vtx[res_id] = vertex_name
    return res_id


class PageRank(object):
    def __init__(self, file_path):
        self.__file_path = file_path
        self.__vtx_to_num = dict()
        self.__num_to_vtx = dict()
        self.__edge_list = []
        self.__vtx_list = []
        self.__alpha = 0.15
        self.__num = 30

    def __read_data(self):
        with open(self.__file_path, 'r') as fin:
            for line in fin.readlines():
                tmp = line.strip().split(' ')
                # 起始节点，即被指向的节点。获取他们的ID
                start = addVertex(tmp[0], self.__vtx_to_num, self.__num_to_vtx)
                for i in range(1, len(tmp)):
                    end = addVertex(tmp[i], self.__vtx_to_num, self.__num_to_vtx)
                    # 标记边的起点和终点
                    self.__edge_list.append(Edge(start, end))

    def __initscore(self):
        # 节点数量
        self.__vtx_num = len(self.__vtx_to_num)
        assert (self.__vtx_num > 0)
        # 初始化各个节点
        self.__vtx_list = [Vertex() for _ in range(self.__vtx_num)]
        # 初始化分数：1 / 节点数量
        for i in range(self.__vtx_num):
            self.__vtx_list[i].score = 1.0 / self.__vtx_num
        # 更新指向和被指向的关系
        for edge in self.__edge_list:
            # 我自己被哪些边指向
            self.__vtx_list[edge.start_id].in_edge.append(edge.end_id)
            # 我指向了哪些边
            self.__vtx_list[edge.end_id].out_edge.append(edge.start_id)
            # 名字
            self.__vtx_list[edge.start_id].name = self.__num_to_vtx[edge.start_id]

        # 假设，3号页面指向 1 号页面和 2 号页面，那么转移到任何一个页面的概率要平分
        for i in range(self.__vtx_num):
            if len(self.__vtx_list[i].out_edge) != 0:
                self.__vtx_list[i].out_score = self.__vtx_list[i].score / len(self.__vtx_list[i].out_edge)

    def run(self):
        self.__read_data()
        self.__initscore()
        # 均匀跳转概率，即跳转到其他页面的概率
        jump_pro = 1 / self.__vtx_num
        # 开始迭代
        print("vertex number is : {}".format(self.__vtx_num))
        print("edge number is : {}".format(len(self.__edge_list)))
        for _ in range(self.__num):
            # 遍历页面，更新分数
            for i in range(self.__vtx_num):
                # 这些页面指向当前页面，计算概率
                s = sum(self.__vtx_list[j].out_score for j in self.__vtx_list[i].in_edge)
                # 再加上跳转概率
                self.__vtx_list[i].score = s * (1 - self.__alpha) + self.__alpha * jump_pro
            # 访问一次后，更新分数
            for i in range(self.__vtx_num):
                if len(self.__vtx_list[i].out_edge) != 0:
                    self.__vtx_list[i].out_score = self.__vtx_list[i].score / len(self.__vtx_list[i].out_edge)

        # 降序
        self.__vtx_list.sort(key=attrgetter('score'), reverse=True)
        return self.__vtx_list


if __name__ == "__main__":
    file_path = "link.data"
    since = time.time()
    p = PageRank(file_path)
    a = p.run()
    end = time.time()
    print(end - since)
    name = [i.name for i in a[:50]]
    score = [i.score for i in a[:50]]
    for i, j in zip(name, score):
        print(i, j)
