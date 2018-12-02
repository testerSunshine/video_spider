import copy
import datetime
import threading

from config.DbTools import MysqlConn
from config.GetProxy import getProxy
from config.RedisUtils import redisUtils
from config.httpClint import HTTPClient
from config.urlConf import urls


class commentThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.httpClint = HTTPClient()
        self.mysqlConn = MysqlConn()
        self.redisConn = redisUtils().redis_conn()

    def run(self):
        t = threading.Thread(target=getProxy, args=(self, ))
        t.setDaemon(True)
        t.start()
        self.getComment()

    def getComment(self):
        """
        获取评论
        :return:
        # """
        # delta = datetime.timedelta(days=1)

        while self.redisConn.llen("movice"):
            movie = eval(self.redisConn.rpop("movice").decode())
            print(movie)
            offset = movie.get("offset", 0)
            # start_time = movie.get("spider_time", datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S'))  # 获取当前时间，从当前时间向前获取
            start_time = self.redisConn.get(movie["nm"]) or datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')  # 获取当前时间，从当前时间向前获取
            while start_time != "done":
                try:
                    commentUrls = copy.copy(urls["comments"])
                    commentUrls["req_url"] = commentUrls["req_url"].format(movie.get("id"), offset, start_time)
                    # offset += 15
                    getCommnetRsp = self.httpClint.send(commentUrls)
                    if getCommnetRsp.get("cmts", ""):
                        self.mysqlConn.insert_comments(getCommnetRsp.get("cmts", ""), movie)
                        start_time = getCommnetRsp.get("cmts", "")[-1]['startTime']  # 获得末尾评论的时间
                        start_time = datetime.datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S') + datetime.timedelta(
                            seconds=-1)  # 转换为datetime类型，减1秒，避免获取到重复数据
                        start_time = datetime.datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S')  # 转换为str
                        print(f"下次爬取评论的时间为：{start_time}")
                        self.redisConn.set(movie["nm"], start_time)
                    elif getCommnetRsp.get("total", "") == 0 or getCommnetRsp.get("cmts", "") == []:  # 如果不返回数据，就代表评论爬到底
                        print("当前页面返回数据为0，判断爬取完成")
                        self.redisConn.set(movie["nm"], "done")
                        break
                except ValueError as e:
                    print(f"日期转化失败: {e}")
                    # movie["offset"] = offset  # 出现问题断点续爬
                    # self.redisConn.lpush("movice", movie)
                    break
                except KeyError as e:
                    print(f"有数据错误：{e}")
                    continue
                except Exception as e:
                    print(f"错误信息：{e}")


if __name__ == '__main__':
    threadingPool = []
    for i in range(6):
        u = commentThread()
        threadingPool.append(u)
    for t in threadingPool:
        t.start()
