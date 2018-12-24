import copy
import threading
import time
from functools import reduce

from config.DbTools import MysqlConn
from config.GetProxy import getProxy
from config.RedisUtils import redisUtils
from config.httpClint import HTTPClient
from config.urlConf import urls


class getMovies(threading.Thread):
    def __init__(self):
        super().__init__()
        self.httpClint = HTTPClient()
        self.redisConn = redisUtils().redis_conn()
        self.movies = "金陵十三钗"

    def run(self):
        self.search()

    def search(self):
        """
        根据电影名字获取该电影id
        :return:
        """
        # t = threading.Thread(target=getProxy, args=(self,))
        # t.setDaemon(True)
        # t.start()
        # time.sleep(2)
        movies_list_all = []
        video_name = self.movies.split("\n")
        for _video_name in video_name:
            searchUrls = copy.copy(urls["search"])
            searchUrls["req_url"] = searchUrls["req_url"].format(_video_name)
            searchRsp = self.httpClint.send(searchUrls)
            if searchRsp.get("movies", ""):
                print(searchRsp["movies"]["list"])
                for movies_list in searchRsp["movies"]["list"]:
                    movies_list_all.append(movies_list)
            else:
                print("暂时没搜索到相关电影资源")
        print(movies_list_all)
        movies_list_all = self.list_dict_duplicate_removal(movies_list_all)
        for i in movies_list_all:
            self.redisConn.lpush("movice", dict(i))

    def list_dict_duplicate_removal(self, data_list):
        run_function = lambda x, y: x if y in x else x + [y]
        return reduce(run_function, [[], ] + data_list)


if __name__ == '__main__':
    # mysqlConn = MysqlConn()
    # g = getMovies()
    # video_name = g.movies.split("\n")
    # # for name in video_name:
    # #     sql = "SELECT COUNT(*) FROM video_mouth_word2 WHERE video_name = '{}'".format(name)
    # #     print(name, mysqlConn.execute_m(sql)[0][0])
    # g.search()
    threadingPool = []
    for i in range(1):
        u = getMovies()
        threadingPool.append(u)
    for t in threadingPool:
        t.start()


