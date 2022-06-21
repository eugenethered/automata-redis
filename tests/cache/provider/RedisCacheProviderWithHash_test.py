import logging
import unittest

from cache.provider.RedisCacheProviderWithHash import RedisCacheProviderWithHash


class RedisCacheProviderWithHashTestCase(unittest.TestCase):

    def setUp(self):
        logging.basicConfig(level=logging.INFO)
        logging.getLogger('RedisCacheProvider').setLevel(logging.DEBUG)

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
        cache_provider.delete('test:mv:complex-key-create')
        cache_provider.delete('test:mv:complex-key-delete')
        cache_provider.delete('test:mv:list')
        cache_provider.delete('test:mv:test-config')
        cache_provider.delete('test:mv:test-multi-config')

    def test_should_store_list_of_values_by_each_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1'}
        cache_provider.values_store('test:values:dictionary-simple', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-simple')
        self.assertEqual(values, values_to_store)

    def test_should_store_list_of_values_by_each_key_for_multiple_keys(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1', 'B': 2}
        cache_provider.values_store('test:values:dictionary-simple-multiple', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-simple-multiple')
        self.assertEqual(values, {'A': '1', 'B': '2'}, 'should convert to string values!')

    def test_should_store_list_of_values_using_default_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'A': '1'},
            {'B': '2'},
            {'C': '3'}
        ]
        cache_provider.values_store('test:values:dictionary-multiple-simple-list', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-multiple-simple-list', as_type=list)
        self.assertEqual(values, values_to_store)

    def test_should_store_list_of_values_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]

        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'

        cache_provider.values_store('test:values:dictionary-multiple-complex-key', values_to_store, custom_key=value_custom_key)
        values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key', as_type=list)
        self.assertEqual(values, values_to_store)

    def test_should_update_specific_value_in_list_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'
        cache_provider.values_store('test:values:dictionary-multiple-complex-key-update', values_to_store, custom_key=value_custom_key)
        values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key-update', as_type=list)
        self.assertEqual(values, values_to_store)
        value_to_update = {'name': 'B+', 'context': 'M'}
        cache_provider.values_set_value('test:values:dictionary-multiple-complex-key-update', 'BM', value_to_update)
        updated_values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key-update', as_type=list)
        expected_values = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B+', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        self.assertEqual(updated_values, expected_values)

    def test_should_create_specific_value_in_list_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'
        cache_provider.values_store('test:mv:complex-key-create', values_to_store, custom_key=value_custom_key)
        values = cache_provider.values_fetch('test:mv:complex-key-create', as_type=list)
        self.assertEqual(values, values_to_store)
        value_to_create = {'name': 'D', 'context': 'M'}
        cache_provider.values_set_value('test:mv:complex-key-create', 'DM', value_to_create)
        updated_values = cache_provider.values_fetch('test:mv:complex-key-create', as_type=list)
        expected_values = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'},
            {'name': 'D', 'context': 'M'}
        ]
        self.assertEqual(updated_values, expected_values)

    def test_should_delete_specific_value_in_list_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'
        cache_provider.values_store('test:mv:complex-key-delete', values_to_store, custom_key=value_custom_key)
        values = cache_provider.values_fetch('test:mv:complex-key-delete', as_type=list)
        self.assertEqual(values, values_to_store)
        cache_provider.values_delete_value('test:mv:complex-key-delete', 'BM')
        updated_values = cache_provider.values_fetch('test:mv:complex-key-delete', as_type=list)
        expected_values = [
            {'name': 'A', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        self.assertEqual(updated_values, expected_values)

    def test_should_store_key_list_value(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.values_store('test:mv:list', [['A', 'B'], ['C', 'D']])
        value = cache_provider.values_fetch('test:mv:list', as_type=list)
        self.assertEqual(value, [['A', 'B'], ['C', 'D']])

    def test_should_store_json_data(self):
        config = {
            'name': 'Eugene',
            'last': 'The Red',
            'address': {
                'place': 'on my island'
            }
        }
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.values_store('test:mv:test-config', config)
        value = cache_provider.values_fetch('test:mv:test-config')
        self.assertEqual(config, value)

    def test_should_store_multiples_of_json_data(self):
        config = [{
            'name': 'Eugene',
            'last': 'The Red',
            'address': {
                'place': 'on my island'
            }
        }]
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.values_store('test:mv:test-multi-config', config)
        values = cache_provider.values_fetch('test:mv:test-multi-config', as_type=list)
        self.assertEqual(values, config)


if __name__ == '__main__':
    unittest.main()
