"""
This module handles rollback of records into S3
Rollback would be required in case of changes in schema or query errors
"""


import json
from enum import Enum
import boto3


s3 = boto3.resource('s3', region_name='us-west-2')
BUCKET = "s3-stellar-stream"


class CErrorTypes(Enum):
    ENCRYPTION = 0
    NEW_SCHEMA = 1
    NEW_TABLE = 2
    NEW_COLUMNS = 3
    SCHEMA_DEFINITION = 4
    QUERY = 5


def _put_to_s3(data,file_name):
    s3object = s3.Object(BUCKET, file_name)
    s3object.put(Body=(bytes(json.dumps(data).encode('UTF-8'))))


def error_log(data_log,file_name): #Used to Archive the logs
    location='log'
    s3_location = f'{location}/{file_name}'
    _put_to_s3(data_log,s3_location)
    return


def archive(record,record_name): #Used to Archive the data
    location='archive'
    s3_location = f'{location}/{record_name}'
    _put_to_s3(record,s3_location)
    return

def send_record_to_s3(data, error: CErrorTypes, file_name):
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
    _put_to_s3(data, s3_location)

    return
