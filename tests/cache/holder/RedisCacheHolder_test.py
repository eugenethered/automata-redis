import unittest

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


if __name__ == '__main__':
    unittest.main()
