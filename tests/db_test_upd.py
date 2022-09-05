import unittest
import os
import sqlalchemy
import sys

#import myapp

class TestDB(unittest.TestCase):
   DB_TEST_URL = ""
   def setUp(self):
       url = os.getenv(self.DB_TEST_URL)
       if not url:
           self.skipTest("No database URL set")
       self.engine = sqlalchemy.create_engine(url)

   def test_foobar(self):
        pass
        #self.assertTrue(myapp.store_integer(self.engine, 42))

if __name__ == '__main__':
    if len(sys.argv) > 0:
        TestDB.DB_TEST_URL = sys.argv.pop()
    unittest.main()