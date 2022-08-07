from datetime import datetime
import boto3
from moto import mock_secretsmanager
#   from botocore.exceptions import ClientError
#   import pytest
from dateutil.tz import tzlocal


def boto_client():
    return boto3.client("secretsmanager", region_name="us-west-2")

@mock_secretsmanager
def test_get_secret_value():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="RDS", SecretString="foosecret")
    result = conn.get_secret_value(SecretId="RDS")
    assert result["SecretString"] == "foosecret"
