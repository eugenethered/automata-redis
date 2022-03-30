from typing import TypeVar

import redis
from core.number.BigFloat import BigFloat

T = TypeVar("T")


class RedisCacheProvider:

    def __init__(self, options, auto_connect=True):
        self.server_address = options['REDIS_SERVER_ADDRESS']
        self.server_port = options['REDIS_SERVER_PORT']
        if auto_connect:
            self.redis_client = redis.Redis(host=self.server_address, port=self.server_port, decode_responses=True)

    def can_connect(self):
        try:
            return self.redis_client.ping()
        except redis.exceptions.ConnectionError:
            return False

    def store(self, key, value):
        if type(value) is BigFloat:
            self.redis_client.set(key, str(value))
        else:
            self.redis_client.set(key, value)

    def fetch(self, key, as_type: T = str):
        value = self.redis_client.get(key)
        if as_type is int:
            return int(value)
        if as_type is float:
            return float(value)
        if as_type is BigFloat:
            return BigFloat(value)
        else:
            return value
