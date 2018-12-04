import datetime
import json

from config.RedisUtils import redisUtils

startTime = datetime.datetime.strptime("2012-01-01", '%Y-%m-%d')
delta = datetime.timedelta(days=1)
now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d', )

offset = startTime
num = 0
dates = []
redisConn = redisUtils().redis_conn()

# 当日期增加到六天后的日期，循环结束
# while str(offset.strftime('%Y-%m-%d')) != now:
#     offset += delta
#     num += 1
#     dates.append(offset.strftime('%Y-%m-%d'))
# redisConn.lpush("box_office_id_end", 629426)
# print(num)
# print(redisConn.llen("movice"))
# while redisConn.llen("movice"):
#     print(redisConn.rpop("movice"))


print(redisConn.get("test"))


