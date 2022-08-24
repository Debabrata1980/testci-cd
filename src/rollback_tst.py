"""
This module handles rollback of records into S3
Rollback would be required in case of changes in schema or query errors
"""


import json
from enum import Enum
import boto3


#s3 = boto3.resource('s3', region_name='us-west-2')
#BUCKET = "s3-stellar-stream"
s3_client = boto3.client("s3")

class CErrorTypes(Enum):
    ENCRYPTION = 0
    NEW_SCHEMA = 1
    NEW_TABLE = 2
    NEW_COLUMNS = 3
    SCHEMA_DEFINITION = 4
    QUERY = 5


def _put_to_s3(data: str, bucket: str, file_name:str):
    s3 = boto3.resource('s3', region_name='us-west-2')
    s3object = s3.Object(bucket, file_name)
    resp = s3object.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))
    print(resp)
    return resp


def error_log(data_log:str,bucket: str, file_name:str): #Used to Archive the logs
    location='log'
    s3_location = f'{location}/{file_name}'
    resp1 = _put_to_s3(data_log, bucket, s3_location)
    print(resp1)
    return resp1


def archive(record:str,bucket: str, record_name:str): #Used to Archive the data
    location='archive'
    s3_location = f'{location}/{record_name}'
    resp2 = _put_to_s3(record,bucket , s3_location)
    print(resp2)
    return resp2

def send_record_to_s3(data:str, error: CErrorTypes, bucket:str, file_name:str):
    s3_folder='roll_back'
    if error == error.NEW_SCHEMA:
        location = f'{s3_folder}/new_schema'
    elif error == error.NEW_TABLE:
        location = f'{s3_folder}/new_table'
    elif error == error.NEW_COLUMNS:
        location = f'{s3_folder}/new_columns'
    elif error == error.SCHEMA_DEFINITION:
        location = f'{s3_folder}/unknown'
    elif error == error.QUERY:
        location = f'{s3_folder}/query'
    
    s3_location = f'{location}/{file_name}'
    resp3= _put_to_s3(data,bucket,s3_location)
    print(resp3)
    return resp3
