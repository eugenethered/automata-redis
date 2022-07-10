import unittest
from datetime import datetime

from core.instant.RunInstantHolder import RunInstantHolder
from core.number.BigFloat import BigFloat

from cache.provider.RedisCacheProviderWithTimeSeries import RedisCacheProviderWithTimeSeries


class RedisCacheProviderWithTimeSeriesTestCase(unittest.TestCase):

    def setUp(self):
        self.options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379
        }

    def tearDown(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.delete_timeseries('timeseries-test')
        cache_provider.delete_timeseries('test:ts:big-float', double_precision=True)
        cache_provider.delete_timeseries('test-timeseries-limited-retention')
        cache_provider.delete_timeseries('test-timeseries-big-float-limited-retention', double_precision=True)

    def test_should_store_time_series(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.add_to_timeseries('timeseries-test', 1, 10.00)
        cache_provider.add_to_timeseries('timeseries-test', 2, 11.00)
        cache_provider.add_to_timeseries('timeseries-test', 3, 12.00)
        timeseries_data = cache_provider.get_timeseries_data('timeseries-test', time_from=1, time_to=3)
        expected = [(1, 10.0), (2, 11.0), (3, 12.0)]
        self.assertEqual(expected, timeseries_data)

    def test_should_obtain_time_series_in_reverse_direction(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.add_to_timeseries('timeseries-test', 1, 10.00)
        cache_provider.add_to_timeseries('timeseries-test', 2, 11.00)
        cache_provider.add_to_timeseries('timeseries-test', 3, 12.00)
        timeseries_data = cache_provider.get_timeseries_data('timeseries-test', time_from=1, time_to=3, reverse_direction=True)
        expected = [(3, 12.0), (2, 11.0), (1, 10.0)]
        self.assertEqual(expected, timeseries_data)

    def test_should_not_fail_when_attempting_to_create_timeseries_multiple_times(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertTrue(result, 'Timeseries should be created')
        cache_provider.create_timeseries('timeseries-test', 'price')
        cache_provider.create_timeseries('timeseries-test', 'price')
        self.assertTrue(result, 'Timeseries should already have been created')

    def test_should_delete_timeseries(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('timeseries-test', 'price')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertTrue(result, 'Timeseries should be created')
        cache_provider.delete_timeseries('timeseries-test')
        result = cache_provider.does_timeseries_exist('timeseries-test')
        self.assertFalse(result, 'Timeseries should not exist')

    def test_should_store_time_series_with_big_floats(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('test:ts:big-float', 'price', double_precision=True)
        cache_provider.add_to_timeseries('test:ts:big-float', 1, BigFloat('1000000000.123456789012'))
        cache_provider.add_to_timeseries('test:ts:big-float', 2, BigFloat('2000000000.210987654321'))
        timeseries_data = cache_provider.get_timeseries_data('test:ts:big-float', time_from=1, time_to=2, double_precision=True)
        expected = [(1, BigFloat('1000000000.123456789012')), (2, BigFloat('2000000000.210987654321'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_big_floats_in_reverse(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('test:ts:big-float', 'price', double_precision=True)
        cache_provider.add_to_timeseries('test:ts:big-float', 1, BigFloat('1000000000.123456789012'))
        cache_provider.add_to_timeseries('test:ts:big-float', 2, BigFloat('2000000000.210987654321'))
        timeseries_data = cache_provider.get_timeseries_data('test:ts:big-float', time_from=1, time_to=2, double_precision=True, reverse_direction=True)
        expected = [(2, BigFloat('2000000000.210987654321')), (1, BigFloat('1000000000.123456789012'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_big_floats_having_leading_zeros(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('test:ts:big-float', 'price', double_precision=True)
        cache_provider.add_to_timeseries('test:ts:big-float', 1, BigFloat('1000000000.000000000012'))
        cache_provider.add_to_timeseries('test:ts:big-float', 2, BigFloat('2000000000.010987654321'))
        timeseries_data = cache_provider.get_timeseries_data('test:ts:big-float', time_from=1, time_to=2, double_precision=True)
        expected = [(1, BigFloat('1000000000.000000000012')), (2, BigFloat('2000000000.010987654321'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_big_floats_having_leading_zeros_with_zero_numbers(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('test:ts:big-float', 'price', double_precision=True)
        cache_provider.add_to_timeseries('test:ts:big-float', 1, BigFloat('0.000000000012'))
        cache_provider.add_to_timeseries('test:ts:big-float', 2, BigFloat('0.010987654321'))
        timeseries_data = cache_provider.get_timeseries_data('test:ts:big-float', time_from=1, time_to=2, double_precision=True)
        expected = [(1, BigFloat('0.000000000012')), (2, BigFloat('0.010987654321'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_mixed_big_floats_to_millisecond_times(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        cache_provider.create_timeseries('test:ts:big-float', 'price', double_precision=True)

        RunInstantHolder.initialize(datetime.fromisoformat('2021-04-01T21:16:25.919601+00:00'))
        time_interval_1 = RunInstantHolder.numeric_run_instance('')
        cache_provider.add_to_timeseries('test:ts:big-float', time_interval_1, BigFloat('1000000000.123456789012'))

        RunInstantHolder.initialize(datetime.fromisoformat('2021-04-01T21:16:25.919605+00:00'))
        time_interval_2 = RunInstantHolder.numeric_run_instance('')
        cache_provider.add_to_timeseries('test:ts:big-float', time_interval_2, BigFloat('1000000000.000000000012'))

        timeseries_data = cache_provider.get_timeseries_data('test:ts:big-float', time_from=time_interval_1, time_to=time_interval_2, double_precision=True)
        expected = [(1617311785919601, BigFloat('1000000000.123456789012')), (1617311785919605, BigFloat('1000000000.000000000012'))]
        self.assertEqual(expected, timeseries_data)

    def test_should_store_time_series_with_limited_retention(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        # limit to 100 ms
        timeseries_key = 'test-timeseries-limited-retention'
        cache_provider.create_timeseries(timeseries_key, 'price', limit_retention=100)
        cache_provider.add_to_timeseries(timeseries_key, '*', 10.00)
        cache_provider.add_to_timeseries(timeseries_key, '*', 11.00)
        cache_provider.add_to_timeseries(timeseries_key, '*', 12.00)
        retention_time = cache_provider.get_timeseries_retention_time(timeseries_key)
        self.assertEqual(retention_time, 100)

    def test_should_create_double_precision_time_series_all_with_limited_retention(self):
        cache_provider = RedisCacheProviderWithTimeSeries(self.options)
        timeseries_key = 'test-timeseries-big-float-limited-retention'
        cache_provider.create_timeseries(timeseries_key, 'price', double_precision=True, limit_retention=100)
        retention_time = cache_provider.get_timeseries_retention_time(timeseries_key)
        self.assertEqual(retention_time, 100)
        fraction_retention_time = cache_provider.get_timeseries_retention_time(cache_provider.fraction_key(timeseries_key))
        self.assertEqual(fraction_retention_time, 100)
        leading_zero_fraction_retention_time = cache_provider.get_timeseries_retention_time(cache_provider.fraction_leading_zeros_key(timeseries_key))
        self.assertEqual(leading_zero_fraction_retention_time, 100)


if __name__ == '__main__':
    unittest.main()
