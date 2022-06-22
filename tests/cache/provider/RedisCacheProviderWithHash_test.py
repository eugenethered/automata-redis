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
        cache_provider.delete('test:mv:get')
        cache_provider.delete('test:mv:set-get-direct')

    def test_should_store_list_of_values_by_each_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1'}
        cache_provider.values_store('test:values:dictionary-simple', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-simple', as_type=dict)
        self.assertEqual(values, values_to_store)

    def test_should_store_list_of_values_by_each_key_for_multiple_keys(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = {'A': '1', 'B': 2}
        cache_provider.values_store('test:values:dictionary-simple-multiple', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-simple-multiple', as_type=dict)
        self.assertEqual(values, {'A': '1', 'B': '2'}, 'should convert to string values!')

    def test_should_store_list_of_values_using_default_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'A': '1'},
            {'B': '2'},
            {'C': '3'}
        ]
        cache_provider.values_store('test:values:dictionary-multiple-simple-list', values_to_store)
        values = cache_provider.values_fetch('test:values:dictionary-multiple-simple-list')
        self.assertEqual(values, values_to_store)

    def test_should_obtain_value_using_default_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'A': '1'},
            {'B': '2'},
            {'C': '3'}
        ]
        cache_provider.values_store('test:mv:get', values_to_store)
        value = cache_provider.values_get_value('test:mv:get', 'B')
        self.assertEqual(value, '2')

    def test_should_not_obtain_value_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'A': '1'},
            {'B': '2'},
            {'C': '3'}
        ]
        cache_provider.values_store('test:mv:get', values_to_store)
        value = cache_provider.values_get_value('test:mv:get', 'Z')
        self.assertIsNone(value)

    def test_should_not_obtain_value_using_specified_key_for_store_which_contains_no_values(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        value = cache_provider.values_get_value('test:mv:get-without-values', 'AAA')
        self.assertIsNone(value)

    def test_should_store_list_of_values_using_specified_key(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        values_to_store = [
            {'name': 'A', 'context': 'M'},
            {'name': 'B', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]

        value_custom_key = lambda value: f'{value["name"]}{value["context"]}'

        cache_provider.values_store('test:values:dictionary-multiple-complex-key', values_to_store, custom_key=value_custom_key)
        values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key')
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
        values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key-update')
        self.assertEqual(values, values_to_store)
        value_to_update = {'name': 'B+', 'context': 'M'}
        cache_provider.values_set_value('test:values:dictionary-multiple-complex-key-update', 'BM', value_to_update)
        updated_values = cache_provider.values_fetch('test:values:dictionary-multiple-complex-key-update')
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
        values = cache_provider.values_fetch('test:mv:complex-key-create')
        self.assertEqual(values, values_to_store)
        value_to_create = {'name': 'D', 'context': 'M'}
        cache_provider.values_set_value('test:mv:complex-key-create', 'DM', value_to_create)
        updated_values = cache_provider.values_fetch('test:mv:complex-key-create')
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
        values = cache_provider.values_fetch('test:mv:complex-key-delete')
        self.assertEqual(values, values_to_store)
        cache_provider.values_delete_value('test:mv:complex-key-delete', 'BM')
        updated_values = cache_provider.values_fetch('test:mv:complex-key-delete')
        expected_values = [
            {'name': 'A', 'context': 'M'},
            {'name': 'C', 'context': 'M'}
        ]
        self.assertEqual(updated_values, expected_values)

    def test_should_store_key_list_value(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.values_store('test:mv:list', [['A', 'B'], ['C', 'D']])
        value = cache_provider.values_fetch('test:mv:list')
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
        value = cache_provider.values_fetch('test:mv:test-config', as_type=dict)
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
        values = cache_provider.values_fetch('test:mv:test-multi-config')
        self.assertEqual(values, config)

    def test_should_set_and_get_value_directly(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        cache_provider.values_set_value('test:mv:set-get-direct', 'A', 1)
        value = cache_provider.values_get_value('test:mv:set-get-direct', 'A')
        # deserialization to type responsible on employer
        self.assertEquals(value, '1')

    def test_should_set_and_get_values_of_list_directly(self):
        cache_provider = RedisCacheProviderWithHash(self.options)
        value = ['X', '0A']
        cache_provider.values_set_value('test:mv:list-value', 'A', value)
        stored_value = cache_provider.values_get_value('test:mv:list-value', 'A')
        # deserialization to type responsible on employer
        self.assertEquals(stored_value, value)


if __name__ == '__main__':
    unittest.main()
