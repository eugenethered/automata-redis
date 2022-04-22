import unittest
from datetime import datetime

from core.constants.not_available import NOT_AVAILABLE
from core.instant.RunInstantHolder import RunInstantHolder
from core.number.BigFloat import BigFloat

from cache.provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheProviderTestCase(unittest.TestCase):

    def setUp(self):
        self.options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379
        }

    def tearDown(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.delete_timeseries('timeseries-test')
        cache_provider.delete_timeseries('timeseries-big-float-test', double_precision=True)

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
        self.assertEqual(cache_provider.fetch('unknown-key', dict), [], 'dict (json) should be None')

    def test_should_fetch_key_names(self):
        cache_provider = RedisCacheProvider(self.options)
        keys = cache_provider.get_keys()
        self.assertGreater(len(keys), 0)

    def test_should_fetch_key_names_matching_pattern(self):
        cache_provider = RedisCacheProvider(self.options)
        keys = cache_provider.get_keys('test-*')
        self.assertGreater(len(keys), 0)


if __name__ == '__main__':
    unittest.main()
