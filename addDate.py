import datetime
import json
from functools import reduce

from config.RedisUtils import redisUtils

startTime = datetime.datetime.strptime("2012-01-01", '%Y-%m-%d')
print(startTime)
delta = datetime.timedelta(days=1)
now = datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d', )

offset = startTime
num = 0
dates = []
redisConn = redisUtils().redis_conn()

# 当日期增加到六天后的日期，循环结束
while str(offset.strftime('%Y-%m-%d')) != now:
    offset += delta
    num += 1
    dates.append(offset.strftime('%Y-%m-%d'))
print(dates)

# redisConn.lpush("box_office_id_end", 629426)
# print(num)
# print(redisConn.llen("movice"))
# movices = []
# while redisConn.llen("movice"):
#     movices.append(redisConn.rpop("movice").decode())
#
# # movices = redisConn.lrange("movice", 0, -1)
#
#
# def list_dict_duplicate_removal(data_list):
#     run_function = lambda x, y: x if y in x else x + [y]
#     return reduce(run_function, [[], ] + data_list)
#
#
# movices = list_dict_duplicate_removal(movices)
# print(movices)
# redisConn.lpush("movice", movices)
# # print(len(movices))
# print(len(set(movices)))


