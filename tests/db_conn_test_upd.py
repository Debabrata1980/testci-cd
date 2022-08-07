# import unittest
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
from moto import mock_secretsmanager,mock_rds
import sure


@mock_secretsmanager
def test_get_secret_value():

    os.environ['RDS'] = 'stellarbi/rds'
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name=os.environ.get('RDS'), SecretString="foosecret")
    result = conn.get_secret_value(SecretId=os.environ.get('RDS'))
    assert result["SecretString"] == "foosecret"


@mock_rds
def test_start_database():
    conn = boto3.client("rds", region_name="us-west-2")
    database = conn.create_db_instance(
        DBInstanceIdentifier="db-master-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName="staging-postgres",
        DBInstanceClass="db.m1.small",
        LicenseModel="license-included",
        MasterUsername="root",
        MasterUserPassword="hunter2",
        Port=1234,
        DBSecurityGroups=["my_sg"],
    )
    mydb = conn.describe_db_instances(
        DBInstanceIdentifier=database["DBInstance"]["DBInstanceIdentifier"]
    )["DBInstances"][0]
    print(mydb)
    mydb["DBInstanceStatus"].should.equal("available")
