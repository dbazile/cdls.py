import unittest
import sys
import datetime

sys.path.append("/Users/david/code/python/CDLS")
import cdls.db

class TestDatabase(unittest.TestCase):
	def setUp(self):
		cdls.db.initialize_db()

	# def tearDown(self):
	# 	cdls.db.reset_db()

	def test_seed_and_query(self):
		import datetime
		cdls.db.warehouse({"foo":"bar"}, "fake", datetime.datetime.now())

if "__main__" == __name__:
	unittest.main()
