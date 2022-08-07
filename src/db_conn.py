"""Module handles establishment of connection to Postgres database"""


import json
import psycopg2
import boto3
import os

sm_client = boto3.client('secretsmanager', region_name='us-west-2')


def db_conn():
    pg_credential = json.loads(sm_client.get_secret_value(SecretId=os.environ['RDS'])['SecretString'])
    return psycopg2.connect(host=pg_credential.get('host'),
                            port=pg_credential.get('port'),
                            user=pg_credential.get('username'),
                            password=pg_credential.get('password'),
                            database=pg_credential.get('dbname'))