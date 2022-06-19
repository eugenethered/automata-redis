import logging

from cache.provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheProviderWithHash(RedisCacheProvider):

    def __init__(self, options, auto_connect=True):
        self.log = logging.getLogger('RedisCacheProviderWithHash')
        super().__init__(options, auto_connect)

    def store_values(self, key, values: dict):
        self.log.debug(f'storing values for key:{key}')
        for k, v in values.items():
            self.redis_client.hset(key, k, v)

    def fetch_values(self, key):
        self.log.debug(f'fetching values for key:{key}')
        return self.redis_client.hgetall(key)
