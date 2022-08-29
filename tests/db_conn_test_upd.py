# import unittest
import boto3
import os
import unittest
# import json
import psycopg2
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
# import src.db_conn


class MyUnitTest(unittest.TestCase):
    @mock_secretsmanager
    def test_get_secret_value(self):
        os.environ['RDS'] = 'stellarbi/rds'
        conn = boto3.client("secretsmanager", region_name="us-west-2")

        conn.create_secret(Name=os.environ.get('RDS'), SecretString="foosecret")
        result = conn.get_secret_value(SecretId=os.environ.get('RDS'))
        assert result["SecretString"] == "foosecret"

    @mock_rds
    def test_start_database(self):
#        from src.db_conn import pg_credential
         conn = boto3.client("rds", region_name="us-west-2")
#        database = conn.create_db_instance(
#            DBInstanceIdentifier="db-master-1",
#            AllocatedStorage=10,
#            Engine="postgres",
#            DBName=pg_credential.get('dbname'),
#            DBInstanceClass="db.m1.small",
#            LicenseModel="license-included",
#            MasterUsername=pg_credential.get['username'],
#            MasterUserPassword=pg_credential.get('password'),
#            Port=pg_credential.get('port'),
#            DBSecurityGroups=["my_sg"],
#        )

         database = conn.create_db_instance(
            DBInstanceIdentifier="db-master-1",
            AllocatedStorage=10,
            Engine="postgres",
            DBName="test",
            DBInstanceClass="db.m1.small",
            LicenseModel="license-included",
            MasterUsername="test-db-user",
            MasterUserPassword="test1234",
            Port=5432,
            DBSecurityGroups=["my_sg"]
        )

         mydb = conn.describe_db_instances(
              DBInstanceIdentifier=database["DBInstance"]["DBInstanceIdentifier"]
          )["DBInstances"][0]
         
         mydb["DBInstanceStatus"].should.equal("available")
         
         print(mydb)

    #    connection = db_conn()   # How I will check to connect to the database I have created by this function of db_conn
         """ create tables in the PostgreSQL database"""
         commands = (
            """
            CREATE TABLE vendors (
                vendor_id SERIAL PRIMARY KEY,
                vendor_name VARCHAR(255) NOT NULL
            )
            """,
            """ 
            CREATE TABLE parts (
                    part_id SERIAL PRIMARY KEY,
                    part_name VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE part_drawings (
                    part_id INTEGER PRIMARY KEY,
                    file_extension VARCHAR(5) NOT NULL,
                    drawing_data BYTEA NOT NULL,
                    FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE vendor_parts (
                    vendor_id INTEGER NOT NULL,
                    part_id INTEGER NOT NULL,
                    PRIMARY KEY (vendor_id , part_id),
                    FOREIGN KEY (vendor_id)
                        REFERENCES vendors (vendor_id)
                        ON UPDATE CASCADE ON DELETE CASCADE,
                    FOREIGN KEY (part_id)
                        REFERENCES parts (part_id)
                        ON UPDATE CASCADE ON DELETE CASCADE
              )
             """)

         host = mydb["Endpoint"]["Address"]

         connection = psycopg2.connect(host=host,
                            port=5432,
                            user="test-db-user",
                            password="test1234",
                            database="test")

         cur = connection.cursor()
            # create table one by one
         for command in commands:
                cur.execute(command)
            # close communication with the PostgreSQL database server
         cur.close()
            # commit the changes
         connection.commit()
             
         response = conn.stop_db_instance(
            DBInstanceIdentifier=mydb["DBInstanceIdentifier"],
            DBSnapshotIdentifier="rocky4570-rds-snap",
           )
         response["DBInstance"]["DBInstanceStatus"].should.equal("stopped")

         conn.delete_db_instance(
            DBInstanceIdentifier="db-master-1",
            FinalDBSnapshotIdentifier="primary-1-snapshot",
          )


if __name__ == '__main__':
    unittest.main()
