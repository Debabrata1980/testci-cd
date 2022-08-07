import unittest
import boto3
import os
# import json
# import psycopg2
# import db_conn
# import botocore
# from botocore.exceptions import ClientError
# import botocore.session
# from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import mock_rds
from db_conn import db_conn, get_data
from unittest.mock import patch


class TestConnection(unittest.TestCase):

    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name=os.environ['RDS'], SecretString="foosecret")
    result = conn.get_secret_value(SecretId="java-util-test-password")
    assert result["SecretString"] == "foosecret"
    
    
    
# Happy scenario always pass
    @patch("db_conn.db_conn")
    def test_db_conn(self, mock_db):
      mock_db.return_value.query_all_data.return_value = 'result data'
      result = get_data()
      self.assertEqual(result, 'result data')
      self.assertEqual(mock_db.call_count, '1')
      self.assertEqual(mock_db.query_all_data.call_count, 1)


if __name__ == '__main__':
    unittest.main()
