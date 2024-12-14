import redis
from typing import Optional, Union
import logging


class RedisClient:
    """
    RedisClient 是一个封装 Redis 连接和常用操作的客户端类。
    提供灵活的配置和统一的接口，简化 Redis 操作。
    """

    def __init__(
        self,
        host: str = "localhost",
        port: int = 6379,
        db: int = 0,
        password: Optional[str] = None,
        timeout: int = 5,
        connection_pool: Optional[redis.ConnectionPool] = None,
    ):
        """
        初始化 Redis 客户端。

        :param host: Redis 服务的主机地址，默认为 localhost。
        :param port: Redis 服务的端口，默认为 6379。
        :param db: 使用的 Redis 数据库，默认为 0。
        :param password: Redis 密码，默认为 None。
        :param timeout: 连接超时设置，默认为 5 秒。
        :param connection_pool: Redis 连接池，如果不传则使用默认连接池。
        """
        self.host = host
        self.port = port
        self.db = db
        self.password = password
        self.timeout = timeout
        self.connection_pool = connection_pool

        # 设置 Redis 连接池
        self.pool = self._get_connection_pool()

        # 初始化 Redis 客户端
        self.client = redis.StrictRedis(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            socket_timeout=self.timeout,
            connection_pool=self.pool,
        )
        self._check_connection()

    def _get_connection_pool(self) -> redis.ConnectionPool:
        """
        获取 Redis 连接池，如果传递了连接池，则使用传递的连接池，
        否则使用默认的连接池。
        """
        if self.connection_pool:
            return self.connection_pool
        return redis.ConnectionPool(
            host=self.host,
            port=self.port,
            db=self.db,
            password=self.password,
            socket_timeout=self.timeout,
        )

    def _check_connection(self):
        """
        尝试连接 Redis 服务并验证连接是否成功。
        """
        try:
            # Ping Redis 服务器
            if self.client.ping():
                logging.info("Successfully connected to Redis.")
        except redis.ConnectionError:
            logging.error("Failed to connect to Redis.")
            raise

    def set(self, key: str, value: Union[str, bytes], ex: Optional[int] = None) -> bool:
        """
        设置 Redis 键值对。

        :param key: 键。
        :param value: 值。
        :param ex: 可选的过期时间（秒）。
        :return: 成功返回 True，否则返回 False。
        """
        try:
            return self.client.set(key, value, ex=ex)
        except redis.RedisError as e:
            logging.error(f"Error setting value for key {key}: {e}")
            return False
    def incr(self, key: str) -> bool:
        """
        设置 Redis 键值对。
        :param key: 键。
        :param value: 值。
        :param ex: 可选的过期时间（秒）。
        :return: 成功返回 True，否则返回 False。
        """
        try:
            return self.client.incrby(key,1)
        except redis.RedisError as e:
            logging.error(f"Error setting value for key {key}: {e}")
            return False
    def setex(self, key: str, value: Union[str, bytes], ex: Optional[int] = None) -> bool:
        """
        设置 Redis 键值对。

        :param key: 键。
        :param value: 值。
        :param ex: 可选的过期时间（秒）。
        :return: 成功返回 True，否则返回 False。
        """
        try:
            return self.client.setex(key, ex, value)
        except redis.RedisError as e:
            logging.error(f"Error setting value for key {key}: {e}")
            return False

    def get(self, key: str) -> Optional[str]:
        """
        获取 Redis 中的值。

        :param key: 键。
        :return: 如果键存在，返回值；否则返回 None。
        """
        try:
            value = self.client.get(key)
            return value.decode("utf-8") if value else None
        except redis.RedisError as e:
            logging.error(f"Error getting value for key {key}: {e}")
            return None

    def delete(self, key: str) -> bool:
        """
        删除 Redis 中的键值对。

        :param key: 键。
        :return: 删除成功返回 True，否则返回 False。
        """
        try:
            return self.client.delete(key) > 0
        except redis.RedisError as e:
            logging.error(f"Error deleting key {key}: {e}")
            return False

    def exists(self, key: str) -> bool:
        """
        检查 Redis 中是否存在指定键。

        :param key: 键。
        :return: 如果存在返回 True，否则返回 False。
        """
        try:
            return self.client.exists(key)
        except redis.RedisError as e:
            logging.error(f"Error checking existence for key {key}: {e}")
            return False

    def keys(self, pattern: str = "*") -> list:
        """
        获取 Redis 中符合模式的所有键。

        :param pattern: 匹配模式，默认为 "*"。
        :return: 键的列表。
        """
        try:
            return self.client.keys(pattern)
        except redis.RedisError as e:
            logging.error(f"Error fetching keys with pattern {pattern}: {e}")
            return []

    def expire(self, key: str, ex: Optional[int] = None) -> list:
        """
        获取 Redis 中符合模式的所有键。

        :param pattern: 匹配模式，默认为 "*"。
        :return: 键的列表。
        """
        try:
            return self.client.expire(key,ex)
        except redis.RedisError as e:
            logging.error(f"Error fetching keys with pattern {key}: {e}")
            return []

    def flushdb(self) -> bool:
        """
        清空当前数据库中的所有键。

        :return: 清空成功返回 True，否则返回 False。
        """
        try:
            return self.client.flushdb()
        except redis.RedisError as e:
            logging.error(f"Error flushing database: {e}")
            return False

    def close(self):
        """
        关闭 Redis 连接。
        """
        try:
            self.client.close()
            logging.info("Redis connection closed.")
        except redis.RedisError as e:
            logging.error(f"Error closing Redis connection: {e}")
