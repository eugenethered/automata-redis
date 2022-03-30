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

    def create_timeseries(self, timeseries_reference, field_name):
        self.redis_timeseries.create(timeseries_reference, labels={'time': field_name})

    def add_to_timeseries(self, timeseries_reference, time, value):
        self.redis_timeseries.add(timeseries_reference, time, value)

    def get_timeseries_data(self, timeseries_reference, time_from, time_to):
        return self.redis_timeseries.range(timeseries_reference, time_from, time_to)
