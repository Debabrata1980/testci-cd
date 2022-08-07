"""Module handles establishment of connection to Postgres database"""


import json
import psycopg2
import boto3
from botocore.exceptions import ClientError

RDS = "SecretDBName1"
sm_client = boto3.client('secretsmanager', region_name='us-west-2')
conn = None


def db_conn():
    try:
        pg_credential = json.loads(sm_client.get_secret_value(SecretId=RDS)['SecretString'])
        conn = psycopg2.connect(host=pg_credential.get('host'),
                                port=pg_credential.get('port'),
                                user=pg_credential.get('username'),
                                password=pg_credential.get('password'),
                                database=pg_credential.get('dbname'))

        return conn
    except ClientError as e:
        print(e)


def get_date():
        dbconn = db_conn()
#       print(pg_credential)
#        cur = dbconn.cursor()
#        cur.execute('SELECT 1')
#        a = cur.fetchone()[0]
        result = dbconn.query_all_data()
#        print('print the records returned after database connection', a)
        return result


if __name__ == "__main__":

    db_conn()
