"""Module handles establishment of connection to Postgres database"""


import json
import psycopg2


def db_conn(sm_client, rds_name):
    pg_credential = json.loads(sm_client.get_secret_value(SecretId=rds_name)['SecretString'])
    return psycopg2.connect(host=pg_credential.get('host'),
                            port=pg_credential.get('port'),
                            user=pg_credential.get('username'),
                            password=pg_credential.get('password'),
                            database=pg_credential.get('dbname'))
