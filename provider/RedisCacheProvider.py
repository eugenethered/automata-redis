from typing import TypeVar

import redis
from core.number.BigFloat import BigFloat
from redistimeseries.client import Client

T = TypeVar("T")


class RedisCacheProvider:

    def __init__(self, options, auto_connect=True):
        self.server_address = options['REDIS_SERVER_ADDRESS']
        self.server_port = options['REDIS_SERVER_PORT']
        if auto_connect:
            self.redis_client = redis.Redis(host=self.server_address, port=self.server_port, decode_responses=True)
            self.redis_timeseries = Client(self.redis_client)

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

    def delete(self, key):
        self.redis_client.delete(key)

    def create_timeseries(self, timeseries_key, field_name):
        if not self.does_timeseries_exist(timeseries_key):
            self.redis_timeseries.create(timeseries_key, labels={'time': field_name})

    def add_to_timeseries(self, timeseries_key, time, value):
        self.redis_timeseries.add(timeseries_key, time, value)

    def get_timeseries_data(self, timeseries_key, time_from, time_to):
        return self.redis_timeseries.range(timeseries_key, time_from, time_to)

    def does_timeseries_exist(self, timeseries_key):
        try:
            self.redis_timeseries.info(timeseries_key)
            return True
        except redis.exceptions.ResponseError:
            return False

    def delete_timeseries(self, timeseries_key):
        self.delete(timeseries_key)
