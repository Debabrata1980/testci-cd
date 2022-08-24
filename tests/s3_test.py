import boto3
from moto import mock_s3
from src.mymodule import MyModel
from src import *
import unittest
import src.schema_reader_tst


class MyUnitTest(unittest.TestCase):

        BUCKET_NAME = "mybucket"
        FILE_NAME = "db_tables"
        PATH = "./file_bkp"
        PATH_ARCH = "./arch"
        FILE_LOCATION = f'{PATH}/{FILE_NAME}.json'
        FILE_LOCATION_ARCH = f'{PATH_ARCH}/{FILE_NAME}.json'
        
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
            from src.rollback_tst import CErrorTypes, send_record_to_s3, archive
            import json
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.BUCKET_NAME)
#            client = boto3.client('s3', region_name='us-east-1')
            print(self.FILE_LOCATION)
            
            with open(self.FILE_LOCATION, 'rb') as data:
                resp = archive(json.dumps(data).encode(),Bucket=self.BUCKET_NAME, record_name=self.FILE_LOCATION_ARCH)
                print(resp)
                content_length = resp["ResponseMetadata"]["HTTPHeaders"]["content-length"]
                print("Content-Length: {}".format(content_length))
 
               # client.upload_fileobj(data, self.BUCKET_NAME, self.FILE_NAME)
               # resp = client.get_object(Bucket=self.BUCKET_NAME, Key=self.FILE_NAME)
     
        @mock_s3
        def download_from_s3(self):
            read_file = schema_reader_tst(self.FILE_NAME)
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket=self.BUCKET_NAME)
            client = boto3.client('s3', region_name='us-east-1')
            resp1 = read_file._downlaod_file (self.BUCKET_NAME,self.FILE_LOCATION,client=client)

           
if __name__ == '__main__':
    unittest.main()            