import copy

from config.DbTools import MysqlConn
from config.RedisUtils import redisUtils
from config.httpClint import HTTPClient
from config.urlConf import urls

promptions = ["trailers", "weibo", "wechat", "baidu"]


class marketingData:
    """
    获取营销数据
    """
    def __init__(self):
        self.httpClint = HTTPClient()
        self.redisConn = redisUtils().redis_conn()
        self.mysqlConn = MysqlConn()

    def getMarketingData(self):
        for promption in promptions:
            promptionUrls = copy.copy(urls["search"])
            promptionUrls["req_url"] = promptionUrls["req_url"].format()