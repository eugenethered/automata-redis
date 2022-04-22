import unittest

from core.options.exception.MissingOptionError import MissingOptionError

from cache.holder.RedisCacheHolder import RedisCacheHolder


class RedisCacheHolderTestCase(unittest.TestCase):

    def test_should_initialize_only_one_redis_cache_provider_instance(self):
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379,
            'AUTO_CONNECT': False
        }
        cache_holder = RedisCacheHolder(options)
        self.assertIsNotNone(cache_holder)
        instance_1 = id(RedisCacheHolder(options))
        instance_2 = id(RedisCacheHolder(options))
        self.assertEqual(instance_1, instance_2, 'every instance after should be the same')

    def test_should_not_require_redis_cache_provider_options_on_subsequent_calls(self):
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379,
            'AUTO_CONNECT': False
        }
        cache_holder = RedisCacheHolder(options)
        self.assertIsNotNone(cache_holder)
        instance_1 = id(RedisCacheHolder())
        instance_2 = id(RedisCacheHolder())
        self.assertEqual(instance_1, instance_2, 'every instance after should be the same')

    def test_should_raise_error_when_options_are_missing_and_auto_connect_should_be_false(self):
        with self.assertRaises(MissingOptionError) as mo:
            RedisCacheHolder.re_initialize()
            RedisCacheHolder(None)
        self.assertEqual('missing option please provide options REDIS_SERVER_ADDRESS and REDIS_SERVER_PORT', str(mo.exception))

    def test_should_not_raise_options_errors_when_not_to_auto_connect(self):
        options = {
            'AUTO_CONNECT': False
        }
        RedisCacheHolder.re_initialize()
        cache = RedisCacheHolder(options)
        self.assertEqual(False, cache.auto_connect)

    def test_should_raise_error_when_redis_server_options_are_missing(self):
        options = {}
        with self.assertRaises(MissingOptionError) as mo:
            RedisCacheHolder.re_initialize()
            RedisCacheHolder(options)
        self.assertEqual('missing option please provide option REDIS_SERVER_ADDRESS', str(mo.exception))

    def test_should_raise_error_when_redis_server_port_missing(self):
        options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90'
        }
        with self.assertRaises(MissingOptionError) as mo:
            RedisCacheHolder.re_initialize()
            RedisCacheHolder(options)
        self.assertEqual('missing option please provide option REDIS_SERVER_PORT', str(mo.exception))


if __name__ == '__main__':
    unittest.main()
