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
# import mock_rds
# import src.db_conn
# from unittest.mock import patch
from moto import mock_secretsmanager


@mock_secretsmanager
class TestConnection(unittest.TestCase):

    os.environ['RDS'] = "stellarbi/rds"
    print(os.environ.get('RDS'))
    client = boto3.client("secretsmanager", region_name="us-west-2")
    client.create_secret(Name="stellarbi/rds", SecretString="foosecret")
    result = client.get_secret_value(SecretId="stellarbi/rds")
    assert result["SecretString"] == "foosecret"


# Happy scenario always pass
#    @patch("db_conn.db_conn")
#    def test_db_conn(self, mock_db):
#      mock_db.return_value.query_all_data.return_value = 'result data'
#      result = get_data()
#      self.assertEqual(result, 'result data')
#      self.assertEqual(mock_db.call_count, '1')
#      self.assertEqual(mock_db.query_all_data.call_count, 1)

if __name__ == '__main__':
    unittest.main()
