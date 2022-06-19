import logging
import unittest

from cache.provider.RedisCacheProviderWithHash import RedisCacheProviderWithHash


class RedisCacheProviderWithHashTestCase(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger('RedisCacheProviderWithHash').setLevel(logging.DEBUG)

        self.options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379
        }

    def tearDown(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.delete('test:values:dictionary-simple')
        cache_provider.delete('test:values:dictionary-simple-multiple')

    def test_should_store_list_of_values_by_each_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1'}
        cache_provider.store_values('test:values:dictionary-simple', values_to_store)
        values = cache_provider.fetch_values('test:values:dictionary-simple')
        self.assertEqual(values, values_to_store)

    def test_should_store_list_of_values_by_each_key_for_multiple_keys(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1', 'B': 2}
        cache_provider.store_values('test:values:dictionary-simple-multiple', values_to_store)
        values = cache_provider.fetch_values('test:values:dictionary-simple-multiple')
        self.assertEqual(values, {'A': '1', 'B': '2'}, 'should convert to string values!')


if __name__ == '__main__':
    unittest.main()
