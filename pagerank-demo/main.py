import time, pickle

from tqdm import trange
from typing import List
from operator import attrgetter


class Vertex:
    def __init__(self, name):
        self.name = name
        self.from_nodes = []
        self.in_degree = 0
        self.to_nodes = []
        self.out_degree = 0
        self.rank = 0
        self.out_rank = 0


def get_data(file_path: str):
    graph: List[Vertex] = []  # adjacency list and the index is the id of vertex
    vertex_num = 0
    edge_num = 0
    name_to_id = {}
    
    # insert vertex if not exists, and return the id
    def get_vertex_id(name: str) -> int:
        nonlocal vertex_num
        if name not in name_to_id:
            name_to_id[name] = vertex_num
            vertex_num += 1
            graph.append(Vertex(name))
        return name_to_id[name]
    
    def add_edge(start_id, end_id):
        # start -> end
        graph[start_id].to_nodes.append(end_id)
        graph[start_id].out_degree += 1
        graph[end_id].from_nodes.append(start_id)
        graph[end_id].in_degree += 1
    
    with open(file_path, 'r') as f:
        for line in f:
            vs = line.split()
            # the first vertex is the end vertex
            end = vs[0]
            end_id = get_vertex_id(end)
            for start in vs[1:]:
                start_id = get_vertex_id(start)
                add_edge(start_id, end_id)
                edge_num += 1
    print(f'vertex number is {vertex_num}')
    print(f'edge number is {edge_num}')
    # initialize rank and out_rank
    for vertex in graph:
        vertex.rank = 1 / vertex_num
        if vertex.out_degree != 0:
            vertex.out_rank = vertex.rank / vertex.out_degree
    return graph


def page_rank(graph, alpha, max_iter_num, epsilon):
    vertex_num = len(graph)
    damping_value = (1 - alpha) / vertex_num
    # for dead ends (out degree is 0)
    revise = sum(vertex.rank / vertex_num for vertex in graph if vertex.out_degree == 0)
    
    # process bar
    range_bar = trange(max_iter_num)
    # begin iteration
    for iter_num in range_bar:
        # change of rank in every iteration
        change = 0
        for vertex in graph:
            s = sum(graph[v_id].out_rank for v_id in vertex.from_nodes)
            rank = alpha * (s + revise) + damping_value
            change += abs(vertex.rank - rank)
            vertex.rank = rank
        # set the description of process bar
        range_bar.set_description(f'change: {change:.6f}')
        # update out_rank and revise
        revise = 0
        for vertex in graph:
            if vertex.out_degree != 0:
                vertex.out_rank = vertex.rank / vertex.out_degree
            else:
                revise += vertex.rank / vertex_num
        # if the change of rank is smaller than epsilon, stop the iteration
        # if change < epsilon:
        #     print("code break in iter: {}".format(iter_num))
        #     range_bar.total = iter_num
        #     break
    # close the process bar
    range_bar.close()


if __name__ == '__main__':
    # config
    file_path = 'link.data'
    alpha = 0.85
    max_iter_num = 100  # maximum iteration number
    epsilon = 1e-5  # determines whether the iteration is over
    top_num = 50  # print the top_num vertexes
    
    # the graph is an adjacency list
    graph = get_data(file_path)
    
    # begin compute
    start = time.time()
    page_rank(graph, alpha, 15, epsilon)
    
    # sum rank
    print('sum rank:', sum(vertex.rank for vertex in graph))
    # sort and print
    graph.sort(key=attrgetter('rank'), reverse=True)
    top_50 = graph[:top_num]
    end = time.time()
    print(f'Cost {end - start} seconds')

    print(f'The top {top_num} vertexes are:')
    for i, vertex in enumerate(top_50):
        print(f'{i:2d}: {vertex.name:12}, {vertex.rank}')

    # since = time.time()
    # f = open('ranks_data', 'wb')
    # pickle.dump(graph, f)

    # f = open('ranks_data', 'rb')
    # graph = pickle.load(f)

    # page_rank(graph, alpha, 1, epsilon)
    # graph.sort(key=attrgetter('rank'), reverse=True)
    # top_50 = graph[:top_num]

    # print(time.time() - since)