from datetime import datetime
import boto3
#   from moto import mock_secretsmanager
#   from botocore.exceptions import ClientError
#   import pytest
from dateutil.tz import tzlocal


def boto_client():
    return boto3.client("secretsmanager", region_name="us-west-2")


#   @mock_secretsmanager
def test_empty():
    conn = boto_client()
    secrets = conn.list_secrets()
    print(secrets)
    assert secrets["SecretList"] == []


#   @mock_secretsmanager
def test_list_secrets():
    conn = boto_client()
    conn.create_secret(Name="test-secret", SecretString="foosecret")
    conn.create_secret(
        Name="test-secret-2",
        SecretString="barsecret",
        Tags=[{"Key": "a", "Value": "1"}],
    )
    secrets = conn.list_secrets()
    print(secrets)
    assert secrets["SecretList"][0]["ARN"] is not None
    assert secrets["SecretList"][0]["Name"] == "test-secret"
    assert secrets["SecretList"][0]["SecretVersionsToStages"] is not None
    assert secrets["SecretList"][1]["ARN"] is not None
    assert secrets["SecretList"][1]["Name"] == "test-secret-2"
    assert secrets["SecretList"][1]["Tags"] == [{"Key": "a", "Value": "1"}]
    assert secrets["SecretList"][1]["SecretVersionsToStages"] is not None
    assert secrets["SecretList"][0]["CreatedDate"] <= datetime.now(tz=tzlocal())
    assert secrets["SecretList"][1]["CreatedDate"] <= datetime.now(tz=tzlocal())
    assert secrets["SecretList"][0]["LastChangedDate"] <= datetime.now(tz=tzlocal())
    assert secrets["SecretList"][1]["LastChangedDate"] <= datetime.now(tz=tzlocal())


#   @mock_secretsmanager
def test_get_secret_value():
    conn = boto3.client("secretsmanager", region_name="us-west-2")
    conn.create_secret(Name="java-util-test-password", SecretString="foosecret")
    result = conn.get_secret_value(SecretId="java-util-test-password")
    assert result["SecretString"] == "foosecret"
