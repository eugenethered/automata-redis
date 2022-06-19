import logging
from typing import TypeVar

from coreutility.json.json_utility import as_pretty_json, as_json

from cache.provider.RedisCacheProvider import RedisCacheProvider

T = TypeVar("T")


class RedisCacheProviderWithHash(RedisCacheProvider):

    def __init__(self, options, auto_connect=True):
        self.log = logging.getLogger('RedisCacheProviderWithHash')
        super().__init__(options, auto_connect)

    def store_values(self, key, values, custom_key=None):
        self.log.debug(f'storing values for key:{key}')
        if type(values) is dict:
            for k, v in values.items():
                self.redis_client.hset(key, k, v)
        elif type(values) is list:
            for v in values:
                value_key = next(iter(v)) if (custom_key is None) else custom_key(v)
                serialized_value = as_pretty_json(v, indent=None)
                self.redis_client.hset(key, value_key, serialized_value)

    def fetch_values(self, key, as_type: T = dict):
        self.log.debug(f'fetching values for key:{key}')
        if as_type is dict:
            return self.redis_client.hgetall(key)
        elif as_type is list:
            stored_values = self.redis_client.hgetall(key)
            return list([as_json(v) for k, v in stored_values.items()])
