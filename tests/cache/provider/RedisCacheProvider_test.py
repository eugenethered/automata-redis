import logging
import unittest

from core.constants.not_available import NOT_AVAILABLE
from core.number.BigFloat import BigFloat
from redis.exceptions import DataError

from cache.provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheProviderTestCase(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger('RedisCacheProvider').setLevel(logging.DEBUG)

        self.options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379
        }

    def tearDown(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.delete('test-na-float')
        cache_provider.delete('test-foo')
        cache_provider.delete('test-config')
        cache_provider.delete('test-multi-config')
        cache_provider.delete('test-number')
        cache_provider.delete('test-big-float')
        cache_provider.delete('test-float')
        cache_provider.delete('test-list')
        cache_provider.delete('test:test-list-json')

    def test_should_connect_to_redis_server(self):
        cache_provider = RedisCacheProvider(self.options)
        connected = cache_provider.can_connect()
        self.assertEqual(connected, True)

    def test_should_not_connect_to_redis_server(self):
        options = {
            'REDIS_SERVER_ADDRESS': 'some-where-over-the-mountain',
            'REDIS_SERVER_PORT': 6379
        }
        cache_provider = RedisCacheProvider(options)
        connected = cache_provider.can_connect()
        self.assertEqual(connected, False)

    def test_should_store_key_string_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-foo', 'bar')
        value = cache_provider.fetch('test-foo')
        self.assertEqual(value, 'bar')

    def test_should_store_key_integer_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-number', 10)
        value = cache_provider.fetch('test-number', as_type=int)
        self.assertEqual(value, 10)

    def test_should_store_key_simple_float_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-float', 100.12)
        value = cache_provider.fetch('test-float', as_type=float)
        self.assertEqual(value, 100.12)

    def test_should_not_store_none_value(self):
        with self.assertRaises(DataError):
            cache_provider = RedisCacheProvider(self.options)
            cache_provider.store('test-none', None)

    def test_should_store_unavailable_simple_float_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-na-float', NOT_AVAILABLE)
        value = cache_provider.fetch('test-na-float', as_type=float)
        self.assertEqual(value, NOT_AVAILABLE)

    def test_should_fetch_relative_none_results_when_key_has_not_been_created(self):
        cache_provider = RedisCacheProvider(self.options)
        self.assertEqual(cache_provider.fetch('unknown-key'), None, 'string should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', int), None, 'int should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', float), None, 'float should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', BigFloat), None, 'BigFloat should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', dict), None, 'dict (json) should be None')

    def test_should_fetch_key_names(self):
        cache_provider = RedisCacheProvider(self.options)
        keys = cache_provider.get_keys()
        self.assertGreater(len(keys), 0)

    def test_should_fetch_key_names_matching_pattern(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-foo', 'bar')
        cache_provider.store('test-number', 10)
        keys = cache_provider.get_keys('test-*')
        self.assertGreater(len(keys), 0)

    def test_should_retrieve_not_found_dict_data_as_none(self):
        cache_provider = RedisCacheProvider(self.options)
        dictionary_value = cache_provider.fetch('test-empty-value', as_type=dict)
        self.assertIsNone(dictionary_value)

    def test_should_store_key_large_precision_float_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-big-float', BigFloat('1000000000.123456789012'))
        value = cache_provider.fetch('test-big-float', as_type=BigFloat)
        self.assertEqual(str(value), '1000000000.123456789012')

    def test_should_store_key_large_precision_float_with_fraction_leading_zeros_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-big-float', BigFloat('1000000000.000000000012'))
        value = cache_provider.fetch('test-big-float', as_type=BigFloat)
        self.assertEqual(str(value), '1000000000.000000000012')


if __name__ == '__main__':
    unittest.main()
