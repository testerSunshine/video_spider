import random

from config.DbTools import MysqlConn
from config.RedisUtils import redisUtils
from config.urlConf import urls
from config.httpClint import HTTPClient


class videoDay:
    def __init__(self):
        self.httpClint = HTTPClient()
        self.redisConn = redisUtils().redis_conn()
        self.mysqlConn = MysqlConn()

    def sendVideoByDay(self):
        """
        获取2012.1.1日到昨天的每天电影数据
        :return:
        """
        GetMovieDayBoxOfficeListUrl = urls.get("GetMovieDayBoxOfficeList", "")

        while self.redisConn.llen("offset"):
            _date = self.redisConn.rpop("offset")
            data = {
                "r": random.random(),
                "UserID": "",
                "DateSort": "Day",
                "Date": _date,
                "sDate": _date,
                "eDate": _date,
                "Index": "102,201,202,205,203,211,221,222,606,225,251,801,604",
                "Line": "",
                "City": "",
                "CityLevel": "",
                "ServicePrice": 1,
                "PageIndex": 1,
                "PageSize": 40,
                "Order": 201,
                "OrderType": "DESC"
            }
            GetMovieDayBoxOfficeListRsp = self.httpClint.send(urls=GetMovieDayBoxOfficeListUrl, data=data)
            GetMovieDayBoxOfficeListData = GetMovieDayBoxOfficeListRsp["Data"]["Table2"]
            self.mysqlConn.insert_video_day(GetMovieDayBoxOfficeListData, _date, self.redisConn)


if __name__ == '__main__':
    v = videoDay()
    v.sendVideoByDay()