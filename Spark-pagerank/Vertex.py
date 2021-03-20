class Vertex:
    def __init__(self, name):
        self.name = name
        self.from_nodes = []
        self.in_degree = 0
        self.to_nodes = []
        self.out_degree = 0
        self.rank = 1
        self.out_rank = 0