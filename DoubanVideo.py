import copy

from config.DbTools import MysqlConn
from config.RedisUtils import redisUtils
from config.httpClint import HTTPClient
from config.urlConf import urls


class doubanVideo:
    def __init__(self):
        self.httpClint = HTTPClient()
        self.redisConn = redisUtils().redis_conn()
        self.mysqlConn = MysqlConn()
        self.isDone = True

    def new_search_subjects(self):
        start = 7895
        while self.isDone:
            print(start)
            new_search_subjects_urls = copy.copy(urls["new_search_subjects"])
            new_search_subjects_urls["req_url"] = new_search_subjects_urls["req_url"].format(start)
            start += 1
            new_search_subjects_rsp = self.httpClint.send(new_search_subjects_urls)
            if new_search_subjects_rsp.get("data", ""):
                douban_datas = new_search_subjects_rsp.get("data", {})
                self.mysqlConn.insert_douban_data(douban_datas)
            else:
                self.isDone = False


if __name__ == '__main__':
    d = doubanVideo()
    d.new_search_subjects()