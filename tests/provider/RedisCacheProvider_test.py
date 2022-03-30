import unittest

from core.number.BigFloat import BigFloat

from provider.RedisCacheProvider import RedisCacheProvider


class RedisCacheProviderTestCase(unittest.TestCase):

    def setUp(self):
        self.options = {
            'REDIS_SERVER_ADDRESS': '192.168.1.90',
            'REDIS_SERVER_PORT': 6379
        }

    def tearDown(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.delete('timeseries-test')

    def test_should_set_server_options(self):
        cache_provider = RedisCacheProvider(self.options, auto_connect=False)
        self.assertEqual(cache_provider.server_address, '192.168.1.90')
        self.assertEqual(cache_provider.server_port, 6379)

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
        cache_provider.store('foo', 'bar')
        value = cache_provider.fetch('foo')
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

    def test_should_store_key_large_precision_float_value(self):
        cache_provider = RedisCacheProvider(self.options)
        cache_provider.store('test-big-float', BigFloat('1000000000.123456789012'))
        value = cache_provider.fetch('test-big-float', as_type=BigFloat)
        self.assertEqual(str(value), '1000000000.123456789012')

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


if __name__ == '__main__':
    unittest.main()
