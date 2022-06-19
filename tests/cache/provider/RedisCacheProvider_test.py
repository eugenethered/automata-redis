import logging
import unittest
from datetime import datetime

from core.constants.not_available import NOT_AVAILABLE
from core.instant.RunInstantHolder import RunInstantHolder
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
        cache_provider.delete_timeseries('timeseries-test')
        cache_provider.delete_timeseries('timeseries-big-float-test', double_precision=True)
        cache_provider.delete_timeseries('test-timeseries-limited-retention')
        cache_provider.delete_timeseries('test-timeseries-big-float-limited-retention', double_precision=True)

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

    def test_should_store_key_list_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-list', [['A', 'B'], ['C', 'D']])
        value = cache_provider.fetch('test-list', as_type=list)
        self.assertEqual(value, [['A', 'B'], ['C', 'D']])

    def test_should_not_store_none_value(self):
        with self.assertRaises(DataError):
            cache_provider = RedisCacheProvider(self.options)
            cache_provider.store('test-none', None)

    def test_should_store_unavailable_simple_float_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-na-float', NOT_AVAILABLE)
        value = cache_provider.fetch('test-na-float', as_type=float)
        self.assertEqual(value, NOT_AVAILABLE)

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

    def test_should_store_time_series(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.add_to_timeseries('timeseries-test', 1, 10.00)
        cache_provider.add_to_timeseries('timeseries-test', 2, 11.00)
        cache_provider.add_to_timeseries('timeseries-test', 3, 12.00)
        timeseries_data = cache_provider.get_timeseries_data('timeseries-test', time_from=1, time_to=3)
        expected = [(1, 10.0), (2, 11.0), (3, 12.0)]
        self.assertEqual(expected, timeseries_data)

    def test_should_obtain_time_series_in_reverse_direction(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.add_to_timeseries('timeseries-test', 1, 10.00)
        cache_provider.add_to_timeseries('timeseries-test', 2, 11.00)
        cache_provider.add_to_timeseries('timeseries-test', 3, 12.00)
        timeseries_data = cache_provider.get_timeseries_data('timeseries-test', time_from=1, time_to=3, reverse_direction=True)
        expected = [(3, 12.0), (2, 11.0), (1, 10.0)]
        self.assertEqual(expected, timeseries_data)

    def test_should_not_fail_when_attempting_to_create_timeseries_multiple_times(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertTrue(result, 'Timeseries should be created')
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.create_timeseries('timeseries-test', 'price')
        self.assertTrue(result, 'Timeseries should already have been created')

    def test_should_delete_timeseries(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertTrue(result, 'Timeseries should be created')
        cache_provider.delete_timeseries('timeseries-test')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertFalse(result, 'Timeseries should not exist')

    def test_should_store_time_series_with_big_floats(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-big-float-test', 'price', double_precision=True)
        cache_provider.add_to_timeseries('timeseries-big-float-test', 1, BigFloat('1000000000.123456789012'))
        cache_provider.add_to_timeseries('timeseries-big-float-test', 2, BigFloat('2000000000.210987654321'))
        timeseries_data = cache_provider.get_timeseries_data('timeseries-big-float-test', time_from=1, time_to=2, double_precision=True)
        expected = [(1, BigFloat('1000000000.123456789012')), (2, BigFloat('2000000000.210987654321'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_big_floats_in_reverse(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-big-float-test', 'price', double_precision=True)
        cache_provider.add_to_timeseries('timeseries-big-float-test', 1, BigFloat('1000000000.123456789012'))
        cache_provider.add_to_timeseries('timeseries-big-float-test', 2, BigFloat('2000000000.210987654321'))
        timeseries_data = cache_provider.get_timeseries_data('timeseries-big-float-test', time_from=1, time_to=2, double_precision=True, reverse_direction=True)
        expected = [(2, BigFloat('2000000000.210987654321')), (1, BigFloat('1000000000.123456789012'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_big_floats_having_leading_zeros(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-big-float-test', 'price', double_precision=True)
        cache_provider.add_to_timeseries('timeseries-big-float-test', 1, BigFloat('1000000000.000000000012'))
        cache_provider.add_to_timeseries('timeseries-big-float-test', 2, BigFloat('2000000000.010987654321'))
        timeseries_data = cache_provider.get_timeseries_data('timeseries-big-float-test', time_from=1, time_to=2, double_precision=True)
        expected = [(1, BigFloat('1000000000.000000000012')), (2, BigFloat('2000000000.010987654321'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_mixed_big_floats_to_millisecond_times(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.create_timeseries('timeseries-big-float-test', 'price', double_precision=True)

        RunInstantHolder.initialize(datetime.fromisoformat('2021-04-01T21:16:25.919601+00:00'))
        time_interval_1 = RunInstantHolder.numeric_run_instance('')
        cache_provider.add_to_timeseries('timeseries-big-float-test', time_interval_1, BigFloat('1000000000.123456789012'))

        RunInstantHolder.initialize(datetime.fromisoformat('2021-04-01T21:16:25.919605+00:00'))
        time_interval_2 = RunInstantHolder.numeric_run_instance('')
        cache_provider.add_to_timeseries('timeseries-big-float-test', time_interval_2, BigFloat('1000000000.000000000012'))

        timeseries_data = cache_provider.get_timeseries_data('timeseries-big-float-test', time_from=time_interval_1, time_to=time_interval_2, double_precision=True)
        expected = [(1617311785919601, BigFloat('1000000000.123456789012')), (1617311785919605, BigFloat('1000000000.000000000012'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_json_data(self):
        config = {
            'name': 'Eugene',
            'last': 'The Red',
            'address': {
                'place': 'on my island'
            }
        }
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-config', config)
        value = cache_provider.fetch('test-config', as_type=dict)
        self.assertEqual(config, value)

    def test_should_store_multiples_of_json_data(self):
        config = [{
            'name': 'Eugene',
            'last': 'The Red',
            'address': {
                'place': 'on my island'
            }
        }]
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-multi-config', config)
        value = cache_provider.fetch('test-multi-config', as_type=dict)
        self.assertEqual(config, value)

    def test_should_fetch_relative_none_results_when_key_has_not_been_created(self):
        cache_provider = RedisCacheProvider(self.options)
        self.assertEqual(cache_provider.fetch('unknown-key'), None, 'string should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', int), None, 'int should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', float), None, 'float should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', BigFloat), None, 'BigFloat should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', dict), None, 'dict (json) should be None')
        self.assertEqual(cache_provider.fetch('unknown-key', list), [], 'list (json) should be None')

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

    def test_should_store_time_series_with_limited_retention(self):
        cache_provider = RedisCacheProvider(self.options)
        # limit to 100 ms
        timeseries_key = 'test-timeseries-limited-retention'
        cache_provider.create_timeseries(timeseries_key, 'price', limit_retention=100)
        cache_provider.add_to_timeseries(timeseries_key, '*', 10.00)
        cache_provider.add_to_timeseries(timeseries_key, '*', 11.00)
        cache_provider.add_to_timeseries(timeseries_key, '*', 12.00)
        retention_time = cache_provider.get_timeseries_retention_time(timeseries_key)
        self.assertEqual(retention_time, 100)

    def test_should_create_double_precision_time_series_all_with_limited_retention(self):
        cache_provider = RedisCacheProvider(self.options)
        timeseries_key = 'test-timeseries-big-float-limited-retention'
        cache_provider.create_timeseries(timeseries_key, 'price', double_precision=True, limit_retention=100)
        retention_time = cache_provider.get_timeseries_retention_time(timeseries_key)
        self.assertEqual(retention_time, 100)
        fraction_retention_time = cache_provider.get_timeseries_retention_time(cache_provider.fraction_key(timeseries_key))
        self.assertEqual(fraction_retention_time, 100)
        leading_zero_fraction_retention_time = cache_provider.get_timeseries_retention_time(cache_provider.fraction_leading_zeros_key(timeseries_key))
        self.assertEqual(leading_zero_fraction_retention_time, 100)

    def test_should_retrieve_not_found_dict_data_as_none(self):
        cache_provider = RedisCacheProvider(self.options)
        dictionary_value = cache_provider.fetch('test-empty-value', as_type=dict)
        self.assertIsNone(dictionary_value)

    def test_should_retrieve_not_found_list_data_as_empty_list(self):
        cache_provider = RedisCacheProvider(self.options)
        list_value = cache_provider.fetch('test-empty-value', as_type=list)
        self.assertIsNotNone(list_value)
        self.assertTrue(len(list_value) == 0)

    def test_should_overwrite_values(self):
        cache_provider = RedisCacheProvider(self.options)
        values = [
            {"test": "A", "context": "C1", "other": "111", "description": "Storing Value 1"},
            {"test": "B", "context": "C1", "other": "101", "description": "Storing Value 2"},
            {"test": "C", "context": "C1", "other": "100", "description": "Storing Value 3"}
        ]
        cache_provider.store('test:test-list-json', values)
        stored_values = cache_provider.fetch('test:test-list-json', as_type=list)
        self.assertEqual(len(stored_values), 3)
        values_without_removed = [v for v in values if v['test'] != 'B']
        cache_provider.overwrite_store('test:test-list-json', values_without_removed)
        stored_values_check = cache_provider.fetch('test:test-list-json', as_type=list)
        self.assertEqual(len(stored_values_check), 2)


if __name__ == '__main__':
    unittest.main()
