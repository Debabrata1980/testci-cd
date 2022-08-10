import boto3
from moto import mock_s3
from src.mymodule import MyModel
from src import *
import unittest


class MyUnitTest(unittest.TestCase):

        BUCKET_NAME = "mybucket"
        FILE_NAME = "db_tables"
        PATH = "./file_bkp"
        FILE_LOCATION = f'{PATH}/{FILE_NAME}.json'
        
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
#            from src.schema_reader import _download_file
#            from rollback import CErrorTypes, send_record_to_s3, archive
#            import json
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.BUCKET_NAME)
            client = boto3.client('s3', region_name='us-east-1')
            print(self.FILE_LOCATION)
            with open(self.FILE_LOCATION, 'rb') as data:
                client.upload_fileobj(data, self.BUCKET_NAME, self.FILE_NAME)
            
            ###  Print
            
            resp = client.get_object(Bucket=self.BUCKET_NAME, Key=self.FILE_NAME)
            content_length = resp["ResponseMetadata"]["HTTPHeaders"]["content-length"]
            print("Content-Length: {}".format(content_length))
           
           
if __name__ == '__main__':
    unittest.main()            