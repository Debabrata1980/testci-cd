import unittest
# import boto3
# import json
# import psycopg2
# import db_conn
# import botocore
# from botocore.exceptions import ClientError
# import botocore.session
# from aws_secretsmanager_caching import SecretCache, SecretCacheConfig
import db_conn
from unittest.mock import patch


class TestConnection(unittest.TestCase):

    # connection = None
    # RDS = "SecretDBName1"
    # sm_client = boto3.client('secretsmanager', region_name='us-west-2')
    # client = botocore.session.get_session().create_client('secretsmanager')
    # cache_config = SecretCacheConfig()
    # cache = SecretCache(config=cache_config, client=sm_client)
    # secret = cache.get_secret_string('MySecret')
    # dict_secret = json.loads(secret)
    # def test_db_conn_params(self):
        # pg_credential = json.loads(self.sm_client.get_secret_value(SecretId=self.RDS)['SecretString'])
        # self.assertEqual(pg_credential.get('host'),dict_secret['host'])
        # self.assertEqual(pg_credential.get('port'),dict_secret['port'])
        # self.assertEqual(pg_credential.get('username'),dict_secret['username'])
        # self.assertEqual(pg_credential.get('password'),dict_secret['password'])
        # self.assertEqual(pg_credential.get('dbInstanceIdentifier'),dict_secret['dbInstanceIdentifier'])
    # def test_db_conn_exception(self):
        # self.assertRaises(ClientError, db_conn.db_conn)

# Happy scenario always pass
    @patch('db_conn.db_conn')
    def test_db_conn(self, mock_db):
      mock_db.return_value.query_all_data.return_value = 'result data'
      result = get_data()
      self.assertEqual(result, 'result data')
      self.assertEqual(mock_db.call_count, '1')
      self.assertEqual(mock_db.query_all_data.call_count, 1)


if __name__ == '__main__':
    unittest.main()
