import redis


class redisUtils:
    def __init__(self):
        self.redis_config = {
            "host": "localhost",
            "port": 6379
        }

    def redis_conn(self):
        """
        redis连接池
        :return:
        """
        redis_pool = redis.ConnectionPool(**self.redis_config)
        conn = redis.Redis(connection_pool=redis_pool)
        return conn


if __name__ == '__main__':
    r = redisUtils()
    conn = r.redis_conn()
    conn.set("haha", "1")
    print(conn.get("haha").decode())