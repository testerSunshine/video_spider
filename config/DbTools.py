# -*- coding: utf8 -*-
import datetime
import time

import pymysql
from pymysql import DataError, InternalError

from config import logger
from config.YamlInfo import _get_yaml_local
from testEmoji import filter_emoji


class MysqlConn:
    pymysql.install_as_MySQLdb()

    def __init__(self):
        self.conn = self.mysql_conn()
        self.cur = self.conn.cursor()

    def mysql_conn(self):
        y = _get_yaml_local("config_video.yaml")
        conn = pymysql.connect(
            host=y["db"]["ip"],
            port=y["db"]["port"],
            user=y["db"]["uname"],
            passwd=y["db"]["passwd"],
            db=y["db"]["table"],
            charset="utf8mb4"
        )
        conn.autocommit(1)
        return conn

    def execute_m(self, sql):
        if sql is None:  # sql not None!
            return "please input sql"
        else:
            try:
                startTime = datetime.datetime.now()
                self.cur.execute(sql)
                logger.log(u"数据执行完毕, 耗时{}ms".format((datetime.datetime.now() - startTime).microseconds / 1000))
                return self.cur.fetchall()
            except DataError as e:
                logger.log(e)
            except InternalError as e:
                logger.log(e)
            except pymysql.err.Error as e:
                logger.log(e)
                # time.sleep(5)
                # self.conn = self.mysql_conn()  # mysql 断开连接5S重连
                # self.execute_m(sql)

    def close_session(self):
        self.cur.close()
        self.conn.close()

    def insert_video_day(self, GetMovieDayBoxOfficeListDate, date, redisConn):
        """
        单日票房数据插入，数据结构
        box_office_id = models.IntegerField("影片id", )
        top = models.IntegerField("排名", null=True)
        video_name = models.CharField("影片名称", max_length=64, default=None)
        top_change = models.IntegerField("影片排名变化", null=True)
        tickets_week = models.CharField("单周票房", max_length=64, null=True)
        tickets_day = models.CharField("单周票房", max_length=64, null=True)
        sequential_update = models.CharField("环比变化", max_length=64, null=True)
        sum_box_office = models.CharField("累计票房", max_length=64, null=True)
        avg_box_office = models.IntegerField("平均票价", null=True)
        avg_show_people = models.IntegerField("场均人数", null=True)
        audience_count = models.IntegerField("人数总和", null=True)
        show_count = models.IntegerField("场次", null=True)
        mouth_index = models.IntegerField("口碑指数", null=True)
        release_days = models.IntegerField("上映天数", null=True)
        box_office_time = models.DateTimeField("影片当天时间", auto_now=True, auto_now_add=False, null=True)
        attendance = models.IntegerField("上座率", null=True)
        offer_seat_percent = models.IntegerField("排座占比", null=True)
        offer_video_percent = models.IntegerField("排片占比", default="", null=True)
        box_percent = models.IntegerField("票房占比", default="", null=True)
        data_channel = models.CharField("数据来源", max_length=32, default="艺恩", null=True)
        box_office_create_time = models.DateTimeField("采集时间", auto_now=True, auto_now_add=False)
        :return:
        """
        print("GetMovieDayBoxOfficeListDate", GetMovieDayBoxOfficeListDate)
        sqlParms = ""
        for data in GetMovieDayBoxOfficeListDate:
            box_office_id = self.conn.escape(data.get("DboMovieID", ""))
            box_office_id_end = self.conn.escape(data.get("EntMovieID", ""))
            top = self.conn.escape(data.get("Irank"))
            video_name = self.conn.escape(data.get("MovieName"))
            tickets_day = self.conn.escape(data.get("BoxOffice"))
            sum_box_office = self.conn.escape(data.get("SumBoxOffice", 0))
            avg_box_office = self.conn.escape(data.get("AvgBoxOffice", 0))
            avg_show_people = self.conn.escape(data.get("AvgShowPeople", 0))
            audience_count = self.conn.escape(data.get("AudienceCount", 0))
            show_count = self.conn.escape(data.get("ShowCount", 0))
            release_days = self.conn.escape(data.get("ColumnList", "").split("|")[3])
            box_office_time = self.conn.escape(date)
            attendance = self.conn.escape(data.get("Attendance"))
            offer_seat_percent = self.conn.escape(data.get("OfferSeatPercent", 0))
            box_percent = self.conn.escape(data.get("BoxPercent", 0))
            data_channel = self.conn.escape("艺恩")
            box_office_create_time = self.conn.escape(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
            sqlParms += f"({box_office_id}, {box_office_id_end}, {top}, {video_name}, {tickets_day}, {sum_box_office}, " \
                        f"{avg_box_office}, {avg_show_people}, {audience_count}, {show_count}, {release_days}, {box_office_time}, " \
                        f"{attendance}, {offer_seat_percent}, {box_percent}, {box_office_create_time}, {data_channel}),"
            if bytes(box_office_id_end, encoding="utf8") not in redisConn.lrange("box_office_id_end", 0, -1):
                print(f"电影id: {box_office_id_end} 未在列表内，将被添加进redis")
                redisConn.lpush("box_office_id_end", box_office_id_end)
        sql = "insert into video_boxofficeday (box_office_id, box_office_id_end, top, video_name, tickets_day, sum_box_office, avg_box_office," \
              f"avg_show_people, audience_count, show_count, release_days, box_office_time, attendance, offer_seat_percent, box_percent, box_office_create_time, data_channel) values {sqlParms}".rstrip(
            ",")
        print(sql)
        self.execute_m(sql)

    def insert_video_data(self, video_data, ):
        """
        电影详细数据
        video_name = models.CharField("影片名称", max_length=64, default=None, )
        video_name_en = models.CharField("影片英文名称", max_length=64, default=None, null=True)
        video_cost = models.IntegerField("制片成本", null=True)  ex
        sum_box_office = models.IntegerField("累计票房", null=True)
        point_box_office = models.IntegerField("点映票房", null=True)
        first_day_box_office = models.IntegerField("首映日票房", null=True)
        first_week_box_office = models.IntegerField("首周票房", null=True)
        first_week_end_box_office = models.IntegerField("首周末票房", null=True)
        video_type = models.CharField("制片类型", max_length=128, null=True)
        video_language = models.CharField("影片语言", max_length=32, null=True )  ex
        video_run_time = models.CharField("片长", max_length=32, null=True)
        director = models.CharField("导演", max_length=32, null=True)
        writers = models.CharField("编剧", max_length=32, null=True) ex
        actor = models.CharField("演员", max_length=32, null=True)  ex
        summary = models.TextField("剧情简介", max_length=50000, null=True)
        record_company = models.CharField("制作公司", max_length=256, null=True)
        company_name = models.CharField("发行方", max_length=256, null=True)
        country_name = models.CharField("国家", max_length=256, null=True)
        release_date = models.CharField("发行日期", max_length=32, null=True)
        data_channel = models.CharField("数据来源", max_length=32, default="艺恩")
        box_office_create_time = models.DateTimeField("采集时间", auto_now=True, auto_now_add=False)
        record_id = models.CharField("影片发行记录id", max_length=128, default=None) ex
        record_area = models.CharField("影片制作区域", max_length=128, default=None) ex
        record_date = models.CharField("影片制作日期", max_length=32, default=None) ex
        video_img = models.CharField("影片封面图", max_length=32, default=None) ex
        video_see_type = models.CharField("影片观看类型", max_length=16, default=None) ex
        :param video_data:
        :param date:
        :return:
        """
        print("video_data", video_data)
        video_name = self.conn.escape(video_data.get("MovieName", ""))
        video_name_en = self.conn.escape(video_data.get("EnMovieName", ""))
        sum_box_office = self.conn.escape(video_data.get("BoxOfficeToTal", ""))
        point_box_office = self.conn.escape(video_data.get("BoxOfficePoint", ""))
        first_day_box_office = self.conn.escape(video_data.get("BoxOfficeFirstDay", ""))
        first_week_box_office = self.conn.escape(video_data.get("BoxOfficeFirstWeek", ""))
        first_week_end_box_office = self.conn.escape(video_data.get("BoxOfficeWeekEnd", ""))
        video_type = self.conn.escape(video_data.get("Genre", ""))
        video_run_time = self.conn.escape(video_data.get("Runtime", ""))
        director = self.conn.escape(video_data.get("Director", ""))
        actors = self.conn.escape(video_data.get("actorName"))
        summary = self.conn.escape(video_data.get("Summary", ""))
        record_company = self.conn.escape(video_data.get("CompanyName1", ""))
        company_name = self.conn.escape(video_data.get("CompanyName", ""))
        country_name = self.conn.escape(video_data.get("CountryName", ""))
        record_id = self.conn.escape(video_data["Table5"].get("RecordID", ""))
        record_area = self.conn.escape(video_data["Table5"].get("RecordArea", ""))
        record_date = self.conn.escape(video_data["Table5"].get("RecordDate", ""))
        video_img = self.conn.escape(video_data.get("MovieImg", ""))
        video_see_type = self.conn.escape(video_data.get("Format", ""))
        release_date = self.conn.escape(video_data.get("ReleaseDate", ""))
        data_channel = self.conn.escape("艺恩")
        box_office_create_time = self.conn.escape(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
        sql = f"insert into video_videodata (video_name, video_name_en, sum_box_office, point_box_office, first_day_box_office, first_week_box_office, " \
              f"first_week_end_box_office, video_type, video_run_time, director, actors, summary, record_company, company_name, country_name, " \
              f"record_id, record_area, record_date, video_img, video_see_type, release_date, data_channel, box_office_create_time) VALUES " \
              f"({video_name}, {video_name_en}, {sum_box_office}, {point_box_office}, {first_day_box_office}, {first_week_box_office}," \
              f"{first_week_end_box_office}, {video_type}, {video_run_time}, {director}, {actors}, {summary}, {record_company}," \
              f"{company_name}, {country_name}, {record_id}, {record_area}, {record_date}, {video_img}, {video_see_type}," \
              f"{release_date}, {data_channel}, {box_office_create_time})"
        print(sql)
        self.execute_m(sql)

    def insert_row_piece(self, row_piece_data):
        """
        排片数据
        video_name = models.CharField("影片名称", max_length=64, default=None)
        buy_ticket_index = models.IntegerField("购票指数", null=True)
        ren_zhi_index = models.IntegerField("认知指数", null=True)
        rap_index = models.IntegerField("口碑指数", null=True)
        weibo_topic_name = models.CharField("微博热门话题", max_length=128, default=None, null=True) ex
        weibo_topic_red = models.CharField("阅读量", max_length=16, default=None, null=True) ex
        weibo_topic_forwarding = models.CharField("转发", max_length=16, default=None, null=True)
        weibo_topic_comment = models.CharField("评论数", max_length=16, default=None, null=True)
        data_channel = models.CharField("数据来源", max_length=32, default="艺恩", null=True)
        box_office_create_time = models.DateTimeField("采集时间", auto_now=True, auto_now_add=False, null=True)
        :return:
        """
        print("row_piece_data", row_piece_data)
        video_name = self.conn.escape(row_piece_data.get("MovieName", ""))
        buy_ticket_index = self.conn.escape(row_piece_data.get("BuyTicketIndex", ""))
        ren_zhi_index = self.conn.escape(row_piece_data.get("RenZhiIndex", ""))
        rap_index = self.conn.escape(row_piece_data.get("RapIndex", ""))
        data_channel = self.conn.escape("艺恩")
        box_office_create_time = self.conn.escape(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
        sql = "insert into video_row_piece (video_name, buy_ticket_index, ren_zhi_index, rap_index, data_channel, box_office_create_time) " \
              f"VALUES ({video_name}, {buy_ticket_index}, {ren_zhi_index}, {rap_index}, {data_channel}, {box_office_create_time})"
        print(sql)
        self.execute_m(sql)

    def insert_marketing_data(self, marketing_data):
        """
        营销数据
        video_name = models.CharField("影片名称", max_length=64, default=None)
        weibo_index = models.CharField("微博指数", max_length=128, null=True)
        weichat_index = models.CharField("微信指数", max_length=128, null=True)
        network_index = models.CharField("网络指数", max_length=128, null=True) ex
        news_index = models.CharField("新闻指数", max_length=128, null=True)
        posters_index = models.CharField("海报指数", max_length=128, null=True)
        trailers_index = models.CharField("片花指数", max_length=128, null=True)
        video_man_num = models.IntegerField("性别男", null=True)
        video_woman_num = models.IntegerField("性别女", null=True)
        video_woman_num_tgl = models.IntegerField("性别偏好度女", null=True)
        video_man_num_tgl = models.IntegerField("性别偏好度男", null=True)
        age_distribution = models.TextField("年龄分布情况", max_length=50000, default=None, null=True)
        province_distribution = models.TextField("地区分布情况", max_length=50000, default=None, null=True)
        data_channel = models.CharField("数据来源", max_length=32, default="艺恩")
        box_office_create_time = models.DateTimeField("采集时间", auto_now=True, auto_now_add=False)
        :param marketing_data:
        :return:
        """
        print("marketing_data", marketing_data)
        video_name = self.conn.escape(marketing_data.get("MovieName", ""))
        weibo_index = self.conn.escape(marketing_data.get("Weibo", ""))
        weichat_index = self.conn.escape(marketing_data.get("WeiXinNews", ""))
        news_index = self.conn.escape(marketing_data.get("WebNews", ""))
        posters_index = self.conn.escape(marketing_data.get("MateriaVideo", ""))
        video_man_num = self.conn.escape(marketing_data.get("ManNum", ""))
        video_woman_num = self.conn.escape(marketing_data.get("WomanNum", ""))
        video_woman_num_tgl = self.conn.escape(marketing_data.get("WoManNumTGI", ""))
        video_man_num_tgl = self.conn.escape(marketing_data.get("ManNumTGI", ""))
        age_distribution = self.conn.escape(marketing_data.get("age_distribution", ""))
        province_distribution = self.conn.escape(marketing_data.get("province_distribution", ""))
        data_channel = self.conn.escape("艺恩")
        box_office_create_time = self.conn.escape(
            datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
        sql = "insert into video_marketing_data (video_name, weibo_index, weichat_index, news_index, posters_index, video_man_num," \
              "video_woman_num, video_woman_num_tgl, video_man_num_tgl, age_distribution, province_distribution, data_channel, box_office_create_time) values" \
              f"({video_name}, {weibo_index}, {weichat_index}, {news_index}, {posters_index}, {video_man_num}, {video_woman_num}," \
              f"{video_woman_num_tgl}, {video_man_num_tgl}, {age_distribution}, {province_distribution}, {data_channel}, {box_office_create_time})"
        print(sql)
        self.execute_m(sql)

    def insert_douban_data(self, douban_datas):
        """
        video_name = models.CharField("影片名称", max_length=64, default=None)
        rate = models.CharField("影片名称", max_length=16, default=None, null=True)
        director = models.CharField("导演", max_length=256, default=None)
        casts = models.CharField("演员", max_length=256, default=None)
        connection_url = models.CharField("豆瓣连接", max_length=128, default=None)
        :return:
        """
        sqlParms = ""
        for douban_data in douban_datas:
            video_name = self.conn.escape(douban_data.get("title", ""))
            rate = self.conn.escape(douban_data.get("rate", ""))
            director = self.conn.escape(",".join(douban_data.get("directors", [])))
            casts = self.conn.escape(",".join(douban_data.get("casts", [])))
            connection_url = self.conn.escape(douban_data.get("url", ""))
            data_channel = self.conn.escape("艺恩")
            box_office_create_time = self.conn.escape(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
            sqlParms += f"({video_name}, {rate}, {director}, {casts}, {connection_url}, {data_channel}, {box_office_create_time}),"
        sql = "insert into video_douban_data (video_name, rate, director, casts, connection_url, data_channel, box_office_create_time)" \
              "VALUES {}".format(sqlParms.rstrip(","))

        print(sql)
        self.execute_m(sql)

    def insert_comments(self, commnets_data, movice):
        """
        评论
        video_name = models.CharField("影片名称", max_length=64, default=None)
        video_type = models.CharField("影片类型", max_length=256, default=None)
        video_release_time = models.CharField("影片上映日期", max_length=64, null=True)
        director = models.CharField("导演", max_length=64, default=None)
        fra = models.CharField("属于国家", max_length=64, default=None)
        video_rate = models.CharField("影片评分", max_length=64, default=None, null=True)
        comment_rate = models.CharField("观众评分", max_length=64, default=None, null=True)
        comment_num = models.CharField("评分人数", max_length=64, default=None, null=True)
        comment_content = models.CharField("短评内容", max_length=64, default=None, null=True)
        comment_content_long = models.CharField("长评内容", max_length=64, default=None, null=True)
        comment_content_time = models.CharField("评论时间", max_length=64, default=None, null=True)
        comment_content_lev = models.CharField("评分等级", max_length=64, default=None, null=True)
        comment_id = models.IntegerField("评论id", null=True)
        gender = models.IntegerField("性别", null=True)
        nickName = models.CharField("评论者名字", max_length=64, default=None, null=True)
        cityName = models.CharField("城市", max_length=64, default=None, null=True)
        comment_time = models.CharField("评论时间", max_length=64, default=None, null=True)
        data_channel = models.CharField("数据来源", max_length=32, default="猫眼")
        box_office_create_time = models.DateTimeField("采集时间", auto_now=True, auto_now_add=False)
        :param commnets_data:
        :return:
        """
        sqlParms = ""
        for data in commnets_data:
            video_name = self.conn.escape(movice.get("nm", ""))
            video_type = self.conn.escape(movice.get("cat", ""))
            video_release_time = self.conn.escape(movice.get("rt", ""))
            director = self.conn.escape(movice.get("dir", ""))
            fra = self.conn.escape(movice.get("fra", ""))
            video_rate = self.conn.escape(str(movice.get("sc", "")))
            comment_rate = self.conn.escape(data.get("score", ""))
            comment_content = filter_emoji(self.conn.escape(data.get("content", "")))
            comment_content_time = self.conn.escape(data.get("startTime", ""))
            comment_id = self.conn.escape(data.get("id", 0))
            gender = self.conn.escape(data.get("gender", 3))
            nick_name = self.conn.escape(data.get("nickName", ""))
            city_name = self.conn.escape(data.get("cityName", ""))
            data_channel = self.conn.escape("猫眼")
            box_office_create_time = self.conn.escape(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S', ))
            sqlParms += f"({video_name}, {video_type}, {video_release_time}, {director}, {fra}, {video_rate}, {comment_rate}," \
                        f"{comment_content}, {comment_content_time}, {comment_id}, {gender}, {nick_name}, {city_name}, {data_channel}, {box_office_create_time}),"
        sql = "insert into video_mouth_word2 (video_name, video_type, video_release_time, director, fra, video_rate, comment_rate, " \
              "comment_content, comment_content_time, comment_id, gender, nick_name, city_name, data_channel, box_office_create_time) values {}".format(
            sqlParms.rstrip(","))
        self.execute_m(sql)


if __name__ == "__main__":
    conn = MysqlConn()
    # for id, url in conn.select_for_table("zhihu_topicinfo", "topic_is_spider=0", "id", "topic_little_url"):
    #     print id, url
    sql = "select *, month(box_office_time)from video_boxofficeday ORDER BY sum_box_office DESC ;"
    result = conn.execute_m(sql)
    num = 0
    num1 = 30
    for i in range(len(result)):
        num += 30
    print(result[0, 30])