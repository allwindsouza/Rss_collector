import boto3
from datetime import datetime
import pytz

session = boto3.Session(profile_name="s3-access-role")
s3 = session.client("s3")

# import creds
#
# s3 = boto3.resource('s3', aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
#                     aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
#                     aws_session_token=creds.AWS_SESSION_TOKEN)


def get_last_modified_file_data(bucket_name, folder_name):
    bucket = s3.Bucket(bucket_name)

    last_modified_file = None
    last_modified_time = datetime.min
    aware_last_modified_time = pytz.utc.localize(last_modified_time)

    for obj in bucket.objects.filter(Prefix=folder_name):
        if obj.last_modified > aware_last_modified_time:
            last_modified_file = obj.key
            aware_last_modified_time = obj.last_modified


    if last_modified_file is not None:
        obj = s3.Object(bucket_name, last_modified_file)
        data = obj.get()['Body'].read().decode('utf-8')
        print("Read data from file '{0}' in folder '{1}'".format(last_modified_file, folder_name))
        return data

    else:
        return None


# get_last_modified_file_data(bucket_name="pub-rss-feed-store",
#                             folder_name="Rss_files_v2/f5055")
