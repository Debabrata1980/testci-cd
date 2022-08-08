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
import sure # noqa # pylint: disable=unused-import


@mock_secretsmanager
def test_get_secret_value():
    os.environ['RDS'] = 'stellarbi/rds'
    conn = boto3.client("secretsmanager", region_name="us-west-2")

    conn.create_secret(Name=os.environ.get('RDS'), SecretString="foosecret")
    result = conn.get_secret_value(SecretId=os.environ.get('RDS'))
    assert result["SecretString"] == "foosecret"


@mock_rds
def test_start_database():
    from src.db_conn import pg_credential
    conn = boto3.client("rds", region_name="us-west-2")
    database = conn.create_db_instance(
        DBInstanceIdentifier="db-master-1",
        AllocatedStorage=10,
        Engine="postgres",
        DBName=pg_credential.get('dbname'),
        DBInstanceClass="db.m1.small",
        LicenseModel="license-included",
        MasterUsername=pg_credential.get['username'],
        MasterUserPassword=pg_credential.get('password'),
        Port=pg_credential.get('port'),
        DBSecurityGroups=["my_sg"],
    )
    mydb = conn.describe_db_instances(
        DBInstanceIdentifier=database["DBInstance"]["DBInstanceIdentifier"]
    )["DBInstances"][0]
    print(mydb)
    mydb["DBInstanceStatus"].should.equal("available")

#    connection = db_conn()   # How I will check to connect to the database I have created by this function of db_conn

    response = conn.stop_db_instance(
        DBInstanceIdentifier=mydb["DBInstanceIdentifier"],
        DBSnapshotIdentifier="rocky4570-rds-snap",
    )
    response["DBInstance"]["DBInstanceStatus"].should.equal("stopped")

    conn.delete_db_instance(
        DBInstanceIdentifier="db-master-1",
        FinalDBSnapshotIdentifier="primary-1-snapshot",
    )
