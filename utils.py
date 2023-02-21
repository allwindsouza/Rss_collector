import boto3
from datetime import datetime

s3 = boto3.resource('s3')

def get_last_modified_file_data(bucket_name, folder_name):
    bucket = s3.Bucket(bucket_name)

    last_modified_file = None
    last_modified_time = datetime.min

    for obj in bucket.objects.filter(Prefix=folder_name):
        if obj.last_modified > last_modified_time:
            last_modified_file = obj.key
            last_modified_time = obj.last_modified

    if last_modified_file is not None:
        obj = s3.Object(bucket_name, last_modified_file)
        data = obj.get()['Body'].read().decode('utf-8')

        print("Read data from file '{0}' in folder '{1}'".format(last_modified_file, folder_name))
        return data

    else:
        print("No files found in folder '{0}'".format(folder_name))
        return None
