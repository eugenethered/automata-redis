import logging

from cache.provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheHolder:
    __instance = None

    def __new__(cls, options=None, desired_provider=RedisCacheProvider):
        if cls.__instance is None:
            log = logging.getLogger('RedisCacheHolder')
            log.info(f'Holder obtaining REDIS cache provider with options:{options}')
            auto_connect = cls.set_auto_connect(options)
            cls.__instance = desired_provider(options, auto_connect)
        return cls.__instance

    @staticmethod
    def set_auto_connect(options):
        if options is None:
            return False
        if 'AUTO_CONNECT' not in options:
            return True
        return options['AUTO_CONNECT']

    @staticmethod
    def re_initialize():
        RedisCacheHolder.__instance = None
