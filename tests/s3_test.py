import boto3
from moto import mock_s3,mock_secretsmanager
from src.mymodule import MyModel
from src import *
import unittest
import os
from src.schema_reader_tst import  Schema 
# from IC.stack_params import PARAMS
import tempfile
import json



class MyUnitTest(unittest.TestCase):

        BUCKET_NAME = "mybucket"
        FILE_NAME = "db_tables.json"
        PATH = "./file_bkp"
        PATH_ARCH = "./arch"
        PATH_NEW_SCHEMA = "./new_schema"
        PATH_DOWNLOAD = "./schema"
        FILE_LOCATION = f'{PATH}/{FILE_NAME}'
        S3_FILE_LOCATION_ARCH = f'{PATH_ARCH}/{FILE_NAME}'
        S3_FILE_LOCATION_NEW_SCHEMA = f'{PATH_NEW_SCHEMA}/{FILE_NAME}'
        FILE_LOCATION_DOWNLOAD = f'{PATH_DOWNLOAD}/{FILE_NAME}'
        DEFAULT_SECRET_NAME = "test-secret"
        
        @mock_s3
        def test_my_model_save(self):
            conn = boto3.resource('s3', region_name='us-east-1')
            # We need to create the bucket since this is all in Moto's 'virtual' AWS account
            conn.create_bucket(Bucket=self.BUCKET_NAME)
            model_instance = MyModel('steve', 'is awesome')
            model_instance.save()
            body = conn.Object(self.BUCKET_NAME, 'steve').get()['Body'].read().decode("utf-8")
            assert body == 'is awesome'

        
        @mock_s3
        def test_archive_s3(self):
#            from src.schema_reader_tst import _download_file
 #           from src.rollback_tst import CErrorTypes, send_record_to_s3, archive
            from src.rollback import CErrorTypes, send_record_to_s3, archive
            import json
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.BUCKET_NAME)
#           client = boto3.client('s3', region_name='us-east-1')
            print(self.FILE_LOCATION)
            
#            with open(self.FILE_LOCATION, 'r') as data:
            f=open(self.FILE_LOCATION)
            resp = archive(json.load(f),record_name=self.S3_FILE_LOCATION_ARCH,BUCKET=self.BUCKET_NAME )
            print(resp)
            content_length = resp["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            respone = resp["ResponseMetadata"]["HTTPStatusCode"]
            print("Content-Length: {}".format(content_length))
            assert content_length == '36971'
            assert respone == 200 
 
               # client.upload_fileobj(data, self.BUCKET_NAME, self.FILE_NAME)
               # resp = client.get_object(Bucket=self.BUCKET_NAME, Key=self.FILE_NAME)

        @mock_s3
        def test_send_record_to_s3(self):
#            from src.schema_reader_tst import _download_file
            from src.rollback_tst import CErrorTypes, send_record_to_s3, archive
#            from src.rollback import CErrorTypes, send_record_to_s3, archive
            import json
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.BUCKET_NAME)
#           client = boto3.client('s3', region_name='us-east-1')
            print(self.FILE_LOCATION)
            
#            with open(self.FILE_LOCATION, 'r') as data:
            f=open(self.FILE_LOCATION)
            resp = send_record_to_s3(data=json.load(f),error=CErrorTypes.NEW_SCHEMA, bucket=self.BUCKET_NAME,file_name=self.S3_FILE_LOCATION_NEW_SCHEMA)
            print(resp)
            content_length = resp["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            respone = resp["ResponseMetadata"]["HTTPStatusCode"]
            print("Content-Length: {}".format(content_length))
            assert content_length == '36971'
            assert respone == 200 
     
        @mock_s3
        def test_download_from_s3(self):
            read_file = Schema(self.FILE_NAME)
            conn = boto3.resource('s3', region_name='us-east-1')
            buck=conn.create_bucket(Bucket=self.BUCKET_NAME)
            client = boto3.client('s3', region_name='us-east-1')
            #buck.put_object(Bucket=self.BUCKET_NAME, Key= f'./file_bkp/{self.FILE_NAME}', Body='')
            buck.upload_file(self.FILE_LOCATION,f'dump/{self.FILE_NAME}')            
            resp2 = client.get_object(Bucket=self.BUCKET_NAME, Key=f'dump/{self.FILE_NAME}')
            content_length = resp2["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            print(resp2)
            print("Content-Length: {}".format(content_length))
            with tempfile.TemporaryDirectory() as tmpdir:
                read_file._download_file(f'dump/{self.FILE_NAME}' , self.BUCKET_NAME,tmpdir)
                mock_folder_local_path = os.path.join(tmpdir)
                print(mock_folder_local_path)
                self.assertTrue(os.path.isdir(mock_folder_local_path))            
                result = os.listdir(mock_folder_local_path)
                desired_result = [self.FILE_NAME]
                self.assertCountEqual(result, desired_result)
            #print(resp1)
            #assert os.path.isfile(self.FILE_NAME)

        @mock_s3
        def test_read_data(self):
            read_file = Schema(self.FILE_NAME)
            data=read_file._read_data(f'../file_bkp/{self.FILE_NAME}')
            f=open(self.FILE_LOCATION)
            self.assertEqual(json.load(f), data)

        @mock_secretsmanager
        def test_db_conn(self):
            from src.db_conn import  db_conn 
            conn = boto3.client("secretsmanager", region_name="us-west-2")
            conn.create_secret(Name=DEFAULT_SECRET_NAME, SecretString="teststring")
            # result = conn.get_secret_value(SecretId=DEFAULT_SECRET_NAME)
            # assert result["SecretString"] == "foosecret"
            db_conn_result = db_conn(conn,DEFAULT_SECRET_NAME)
            assert db_conn_result["SecretString"] == "teststring"
            
           
if __name__ == '__main__':
    unittest.main()            
