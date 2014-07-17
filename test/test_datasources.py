import unittest
import sys
import datetime

sys.path.append("/Users/david/code/python/CDLS")
import cdls.datasources

class TestDateParser(unittest.TestCase):
	def setUp():
		print("Setting up")

	def test_iso8601(self):
		date_strings = ["1999-07-04T11:00:00EDT"]
		expected = datetime.datetime(1999, 7, 4, 15, 0, 0, tzinfo=datetime.timezone.utc)

		for date_string in date_strings:
			date_object = cdls.datasources._string_to_date(date_string)

			self.assertIsInstance(date_object, datetime.datetime)
			self.assertEqual(date_object, expected)

if "__main__" == __name__:
	unittest.main()
