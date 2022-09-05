import unittest
import os
import sqlalchemy
import sys

#import myapp

class TestDB(unittest.TestCase):
   DB_TEST_URL = ""
   def setUp(self):
       url = self.DB_TEST_URL
       if not url:
           self.skipTest("No database URL set")
       self.engine = sqlalchemy.create_engine(url)
       self.connection = self.engine.connect()
#       self.connection.execute("CREATE DATABASE testdb")

       
       
       
   def test_foobar(self):
       self.connection.execute("CREATE Table test(id int,name varchar)")
       self.connection.execute("insert into test values (1,'test')")
       self.connection.execute("insert into test values (2,'test1')")
       result = list(self.connection.execute("select id from test "))
       assert result == [(1,), (2,)]
        #self.assertTrue(myapp.store_integer(self.engine, 42))

if __name__ == '__main__':
    if len(sys.argv) > 0:
        TestDB.DB_TEST_URL = sys.argv.pop()
    unittest.main()