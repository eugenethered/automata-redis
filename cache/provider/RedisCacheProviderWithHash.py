from typing import TypeVar

from coreutility.json.json_utility import as_pretty_json, as_json

from cache.provider.RedisCacheProvider import RedisCacheProvider

T = TypeVar("T")


class RedisCacheProviderWithHash(RedisCacheProvider):

    def __init__(self, options, auto_connect=True):
        super().__init__(options, auto_connect)

    def values_store(self, key, values, custom_key=None):
        self.log.debug(f'storing values for key:{key}')
        if type(values) is dict:
            for k, v in values.items():
                if type(v) is dict:
                    serialized_value = as_pretty_json(v, indent=None)
                    self.redis_client.hset(key, k, serialized_value)
                else:
                    self.redis_client.hset(key, k, v)
        elif type(values) is list:
            for v in values:
                value_key = next(iter(v)) if (custom_key is None) else custom_key(v)
                serialized_value = as_pretty_json(v, indent=None)
                self.log.debug(f'storing key:[{value_key}] value:[{serialized_value}]')
                self.redis_client.hset(key, value_key, serialized_value)

    def values_set_value(self, key, value_key, value):
        serialized_value = as_pretty_json(value, indent=None)
        self.redis_client.hset(key, value_key, serialized_value)

    def values_delete_value(self, key, value_key):
        self.redis_client.hdel(key, value_key)

    def values_fetch(self, key, as_type: T = list):
        self.log.debug(f'fetching values for key:{key}')
        if as_type is dict:
            values = self.redis_client.hgetall(key)
            for k, v in values.items():
                if v.startswith('{'):
                    values[k] = as_json(v)
            return values
        elif as_type is list:
            stored_values = self.redis_client.hgetall(key)
            return list([as_json(v) for k, v in stored_values.items()])
