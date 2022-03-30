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

    @staticmethod
    def fraction_key(key):
        return f'{key}:fraction'

    def __create_timeseries(self, key, field_name):
        if not self.does_timeseries_exist(key):
            self.redis_timeseries.create(key, labels={'time': field_name})

    def create_timeseries(self, key, field_name, double_precision=False):
        self.__create_timeseries(key, field_name)
        if double_precision:
            self.__create_timeseries(self.fraction_key(key), field_name)

    def add_to_timeseries(self, key, time, value):
        if type(value) is BigFloat:
            self.redis_timeseries.add(key, time, value.number)
            self.redis_timeseries.add(self.fraction_key(key), time, value.fraction)
        else:
            self.redis_timeseries.add(key, time, value)

    def get_timeseries_data(self, key, time_from, time_to, double_precision=False):
        if double_precision:
            number_values = self.redis_timeseries.range(key, time_from, time_to)
            fraction_values = self.redis_timeseries.range(self.fraction_key(key), time_from, time_to)
            return [(n1, BigFloat(f'{int(v1)}.{int(v2)}')) for (n1, v1), (f2, v2) in zip(number_values, fraction_values) if n1 == f2]
        else:
            return self.redis_timeseries.range(key, time_from, time_to)

    def does_timeseries_exist(self, timeseries_key):
        try:
            self.redis_timeseries.info(timeseries_key)
            return True
        except redis.exceptions.ResponseError:
            return False

    def delete_timeseries(self, key, double_precision=False):
        self.delete(key)
        if double_precision:
            self.delete(self.fraction_key(key))

