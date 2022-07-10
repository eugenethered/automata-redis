import unittest

from core.number.BigFloat import BigFloat

from cache.utility.BigFloat_utility import crack_to_serialize, join_to_deserialize


class BigFloatUtilityTestCase(unittest.TestCase):

    def test_should_crack_bigfloat_into_number_and_decimal(self):
        bigfloat = BigFloat('1000000000.123456789012')
        (number, decimal, leading_decimal_zeros) = crack_to_serialize(bigfloat)
        self.assertEqual(number, 1000000000)
        self.assertEqual(decimal, 123456789012)
        self.assertEqual(leading_decimal_zeros, 0)

    def test_should_crack_bigfloat_into_number_and_decimal_with_decimal_leading_zeros(self):
        bigfloat = BigFloat('1000000000.000000000012')
        (number, decimal, leading_decimal_zeros) = crack_to_serialize(bigfloat)
        self.assertEqual(number, 1000000000)
        self.assertEqual(decimal, 12)
        self.assertEqual(leading_decimal_zeros, 10)

    def test_should_crack_bigfloat_into_number_and_decimal_with_decimal_leading_zeros_and_no_number(self):
        bigfloat = BigFloat('0.000000000012')
        (number, decimal, leading_decimal_zeros) = crack_to_serialize(bigfloat)
        self.assertEqual(number, 0)
        self.assertEqual(decimal, 12)
        self.assertEqual(leading_decimal_zeros, 10)

    def test_should_crack_bigfloat_into_number_and_decimal_with_zero_number(self):
        bigfloat = BigFloat('0.000000000012')
        (number, decimal, leading_decimal_zeros) = crack_to_serialize(bigfloat)
        self.assertEqual(number, 0)
        self.assertEqual(decimal, 12)
        self.assertEqual(leading_decimal_zeros, 10)

    def test_should_crack_bigfloat_into_number_and_decimal_with_zeros(self):
        bigfloat = BigFloat('0.0')
        (number, decimal, leading_decimal_zeros) = crack_to_serialize(bigfloat)
        self.assertEqual(number, 0, 'number should be zero')
        self.assertEqual(decimal, 0, 'decimal should be zero')
        self.assertEqual(leading_decimal_zeros, 0, 'leading decimal zeros should be zero')

    def test_should_join_bigfloat_into_number_and_decimal(self):
        bigfloat = join_to_deserialize(1000000000, 123456789012, 0)
        self.assertEqual(str(bigfloat), '1000000000.123456789012')

    def test_should_join_bigfloat_into_number_and_decimal_with_decimal_leading_zeros(self):
        bigfloat = join_to_deserialize(1000000000, 12, 10)
        self.assertEqual(str(bigfloat), '1000000000.000000000012')

    def test_should_join_bigfloat_into_number_and_decimal_with_zero_number(self):
        bigfloat = join_to_deserialize(0, 12, 10)
        self.assertEqual(str(bigfloat), '0.000000000012')

    def test_should_join_bigfloat_into_number_and_decimal_with_zeros(self):
        bigfloat = join_to_deserialize(0, 0, 0)
        self.assertEqual(str(bigfloat), '0.0')


if __name__ == '__main__':
    unittest.main()
