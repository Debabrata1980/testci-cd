# import unittest
import boto3
# import os
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
def test_get_secret_value():

    os.environ['RDS']= 'stellarbi/rds'
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name=os.environ.get('RDS'), SecretString="foosecret")
    result = conn.get_secret_value(SecretId=os.environ.get('RDS'))
    assert result["SecretString"] == "foosecret"

