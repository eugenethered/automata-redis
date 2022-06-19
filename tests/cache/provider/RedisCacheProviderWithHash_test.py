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
        cache_provider.delete('test:values:dictionary-multiple-simple-list')
        cache_provider.delete('test:values:dictionary-multiple-complex-key')
        cache_provider.delete('test:values:dictionary-multiple-complex-key-update')

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

    def test_should_store_list_of_values_using_default_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'A': '1'},
            {'B': '2'},
            {'C': '3'}
        ]
        cache_provider.store_values('test:values:dictionary-multiple-simple-list', values_to_store)
        values = cache_provider.fetch_values('test:values:dictionary-multiple-simple-list', as_type=list)
        self.assertEqual(values, values_to_store)

    def test_should_store_list_of_values_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]

        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'

        cache_provider.store_values('test:values:dictionary-multiple-complex-key', values_to_store, custom_key=value_custom_key)
        values = cache_provider.fetch_values('test:values:dictionary-multiple-complex-key', as_type=list)
        self.assertEqual(values, values_to_store)

    def test_should_update_specific_value_in_list_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'
        cache_provider.store_values('test:values:dictionary-multiple-complex-key-update', values_to_store, custom_key=value_custom_key)
        values = cache_provider.fetch_values('test:values:dictionary-multiple-complex-key-update', as_type=list)
        self.assertEqual(values, values_to_store)
        value_to_update = {'name': 'B+', 'context': 'M'}
        cache_provider.store_values_value('test:values:dictionary-multiple-complex-key-update', 'BM', value_to_update)
        updated_values = cache_provider.fetch_values('test:values:dictionary-multiple-complex-key-update', as_type=list)
        expected_values = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B+', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        self.assertEqual(updated_values, expected_values)


if __name__ == '__main__':
    unittest.main()
