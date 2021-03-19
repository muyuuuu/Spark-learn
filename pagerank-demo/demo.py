import numpy as np


class pagerank(object):
    def __init__(self, adj, alpha=0.15, num=1000):
        self.__adj = adj
        self.__alpha = alpha
        self.__num = num
        self.__page_num = self.__adj.shape[0]

    def run(self):
        # 均匀设置分数
        score = [1 / self.__page_num for i in range(self.__page_num)]

        # 看看有哪些页面指向自己
        point_to_self = [
            list(np.where(self.__adj[:, i] == 1)[0])
            for i in range(self.__page_num)
        ]

        # 如果四个页面，3号页面指向1号页面和2号页面，那么转移到任何一个页面的概率要平分
        point_to_other = [
            s / np.where(self.__adj[i, :] == 1)[0].size
            for i, s in enumerate(score)
        ]

        # 均匀跳转概率
        jump_pro = 1 / self.__page_num

        # 网页访问 num 次，计算每个页面的概率
        for num in range(self.__num):
            for i in range(self.__page_num):
                s = sum(point_to_other[j] for j in point_to_self[i])
                # 其它页面指向自己，加上跳转概率，就是自己被访问的概率
                score[i] = s * (1 - self.__alpha) + self.__alpha * jump_pro
            # 访问一次后，指向其它页面的概率更新
            point_to_other = [
                s / np.where(self.__adj[i, :] == 1)[0].size
                for i, s in enumerate(score)
            ]
        return score


if __name__ == "__main__":

    adj = np.array([[0, 0, 1, 1], [0, 0, 1, 1], [1, 1, 0, 0], [0, 1, 1, 0]])
    assert adj.shape[0] == adj.shape[1]
    alpha = 0.15
    num = 1000

    p = pagerank(adj, alpha=alpha, num=num)
    score = p.run()

    page_pro = [str(round(i * 100, 2)) + '%' for i in score]
    print(page_pro)