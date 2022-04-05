import logging

from cache.provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheHolder:
    __instance = None

    def __new__(cls, options=None):
        if cls.__instance is None:
            logging.info(f'Holder obtaining REDIS cache provider with options:{options}')
            auto_connect = True if 'AUTO_CONNECT' not in options else options['AUTO_CONNECT']
            cls.__instance = RedisCacheProvider(options, auto_connect)
        return cls.__instance
