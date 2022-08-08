import boto3
from moto import mock_s3
from src.mymodule import MyModel


class MyUnitTest(unittest.TestCase):

        @mock_s3
        def test_my_model_save(self):
            conn = boto3.resource('s3', region_name='us-east-1')
            # We need to create the bucket since this is all in Moto's 'virtual' AWS account
            conn.create_bucket(Bucket='mybucket')
            model_instance = MyModel('steve', 'is awesome')
            model_instance.save()
            body = conn.Object('mybucket', 'steve').get()['Body'].read().decode("utf-8")
            assert body == 'is awesome'

        """
        @mock_s3
        def test_archive_s3(self):
            from src.schema_reader import _download_file
            from rollback import CErrorTypes, send_record_to_s3, archive
        #    import json
            conn = boto3.resource('s3', region_name='us-east-1')
            conn.create_bucket(Bucket='mybucket')
            data = {
              "type": "com.hp.id.data.internal.profile_create.v1",
              "id": "sdfgregfdgh54hyth56hrth56hrhtryj",
              "data": {
                  "profile": {
                      "hpidId": "uniquehpid",
                      "type": "consumer",
                      "username": "email@domain.com",
                      "usernameAlias": [
                          "email@domain.com",
                      ]
                  }
              }
            }
            client = boto3.client('s3', region_name='us-east-1')
            """