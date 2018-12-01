import datetime
import json
import random

from config.DbTools import MysqlConn
from config.RedisUtils import redisUtils
from config.httpClint import HTTPClient
from config.urlConf import urls


class videoDetail:
    def __init__(self):
        self.httpClint = HTTPClient()
        self.redisConn = redisUtils().redis_conn()
        self.mysqlConn = MysqlConn()

    def movieDataByBaseInfo(self):
        """
        获取电影详情
        :return:
        """
        movieDataByBaseInfoUrl = urls.get("MovieDataByBaseInfo", "")
        while self.redisConn.llen("box_office_id_end"):
            EnMovieID = self.redisConn.rpop("box_office_id_end")
            print(EnMovieID)
            data = {
                "r": random.random(),
                "MovieID": "",
                "EnMovieID": EnMovieID,
                "ServicePrice": 1,
            }
            try:
                movieDataByBaseInfoRsp = self.httpClint.send(urls=movieDataByBaseInfoUrl, data=data)
                if movieDataByBaseInfoRsp["Data"]["Table1"]:
                    Table1 = movieDataByBaseInfoRsp["Data"]["Table1"][0]
                    insert_marketing_data = movieDataByBaseInfoRsp["Data"]["Table2"][0]
                    MovieID = Table1["MovieID"]
                    DBOMovieID = Table1["DBOMovieID"]
                    EFMTMovieID = Table1["EFMTMovieID"]

                    movieDataByDetailData = self.movieDataByDetail(MovieID, EnMovieID, EFMTMovieID)
                    movieDataByAudienceData = self.movieDataByAudience(MovieID, EnMovieID, EFMTMovieID, Table1["ReleaseDate"])

                    self.mysqlConn.insert_video_data(dict(Table1, **movieDataByDetailData), )
                    self.mysqlConn.insert_marketing_data(dict(insert_marketing_data, **movieDataByAudienceData))
                    self.mysqlConn.insert_row_piece(insert_marketing_data)
            except:
                self.redisConn.lpush("box_office_id_end", EnMovieID)

    def movieDataByDetail(self, MovieID, EnMovieID, EFMTMovieID):
        """
        电影制作详情
        :param MovieID:
        :param EnMovieID:
        :param EFMTMovieID:
        :return:
        """
        movieDataByDetailUrl = urls.get("MovieDataByDetail", "")
        data = {
            "r": random.random(),
            "MovieID": MovieID,
            "EnMovieID": EnMovieID,
            "EFMTMovieID": EFMTMovieID,
        }
        movieDataByDetailRsp = self.httpClint.send(urls=movieDataByDetailUrl, data=data)
        Table1 = movieDataByDetailRsp["Data"]["Table1"][0]
        Table3 = movieDataByDetailRsp["Data"]["Table3"]
        movieDataByDetailData = Table1
        actorName = []  # 演员json
        CompanyName = []  # 制作方json
        for actor in Table3:
            if actor["PersonType"] == "演员":
                actorName.append(actor)
        for Company in movieDataByDetailRsp["Data"]["Table2"]:
            if Company["CompanyType"] == "制作公司":
                CompanyName.append(Company)
        movieDataByDetailData["actorName"] = json.dumps(actorName)
        movieDataByDetailData["CompanyName1"] = json.dumps(CompanyName)
        movieDataByDetailData["Table5"] = movieDataByDetailRsp["Data"]["Table5"][0] if  movieDataByDetailRsp["Data"]["Table5"] else {}
        return movieDataByDetailData

    def movieDataByAudience(self, MovieID, EnMovieID, EFMTMovieID, date):
        """
        营销指数详情
        :param MovieID:
        :param EnMovieID:
        :param EFMTMovieID:
        :return:
        """
        movieDataByAudienceUrl = urls.get("MovieDataByAudience", "")
        if date:
            startTime = datetime.datetime.strptime(date, '%Y-%m-%d')
            delta = datetime.timedelta(days=20)   # 从上映天数往后面默认抓取20天之内的数据
            eData = (startTime + delta).strftime("%Y-%m-%d")
            data = {
                "MovieID": MovieID,
                "EnMovieID": EnMovieID,
                "EFMTMovieID": EFMTMovieID,
                "DateSort": "Self",
                "Date": f"{date},{eData}",
                "sDate": date,
                "eDate": eData,
            }
            movieDataByAudienceRsp = self.httpClint.send(urls=movieDataByAudienceUrl, data=data)
            try:
                movieDataByAudienceData = movieDataByAudienceRsp["Data"]["Table1"][0]
                movieDataByAudienceData["age_distribution"] = json.dumps(movieDataByAudienceRsp["Data"]["Table2"])
                movieDataByAudienceData["province_distribution"] = json.dumps(movieDataByAudienceRsp["Data"]["Table3"])
                return movieDataByAudienceData
            except:
                return {
                    "age_distribution": "",
                    "province_distribution": ""
                }
        else:
            return {}


if __name__ == '__main__':
    v = videoDetail()
    v.movieDataByBaseInfo()