import csv
import os
import requests
import time
import shutil
import sys
from tempfile import NamedTemporaryFile
import logging
import datetime
import boto3
import hashlib
from pytz import timezone
from xml_utils import compare_xml_files
from utils import get_last_modified_file_data

logging.basicConfig(
    filename='logs.log',
    format='%(asctime)s %(message)s',
    filemode='w'
)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

session = boto3.Session(profile_name="s3-access-role")
s3 = session.client("s3")

# import creds
#
# s3 = boto3.resource('s3', aws_access_key_id=creds.AWS_ACCESS_KEY_ID,
#                     aws_secret_access_key=creds.AWS_SECRET_ACCESS_KEY,
#                     aws_session_token=creds.AWS_SESSION_TOKEN)

bucket = 'pub-rss-feed-store'

filename = 'temp_rss_feed.csv'

skip = ['137']

fields = ['rss_id', 'rss_url', 'md5_hash', 'last_changed', 'change_interval', 'epoch_counter']

while True:
    ind_time = datetime.datetime.now(timezone("Asia/Kolkata")).strftime('%Y-%m-%d %H:%M:%S.%f')

    logger.info(f"Starting to Iterate over the rss URLS at {ind_time}. \n")
    sys.stdout.write(f"Starting to Iterate over the rss URLS at {ind_time}. \n")

    temp_file = NamedTemporaryFile(mode='w', delete=False)
    with open(filename, 'r') as csvfile, temp_file:
        reader = csv.DictReader(csvfile, fieldnames=fields)
        writer = csv.DictWriter(temp_file, fieldnames=fields)

        for row in reader:
            sys.stdout.write("Processing Rss_ID: {} : \n".format(row['rss_id']))
            logger.info("Processing Rss_ID: {} : \n".format(row['rss_id']))

            try:
                url_request = requests.get(row['rss_url'])
                data = url_request.text
                file_hash = hashlib.md5(data.encode('utf-8')).hexdigest()

                last_changed = row['last_changed']
                if last_changed != "null":
                    last_changed = datetime.datetime.strptime(row['last_changed'], '%Y-%m-%d %H:%M:%S.%f')

                if file_hash == row['md5_hash']:
                    sys.stdout.write("\t Same hash as previous iteration: {}. \n".format(file_hash))
                    logger.info("\t Same hash as previous iteration: {}. \n".format(file_hash))
                    new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'],
                               'md5_hash': row['md5_hash'], 'last_changed': row['last_changed'],
                               'change_interval': row['change_interval'], 'epoch_counter': row['epoch_counter']}

                else:
                    # Doing further checks:
                    if not row['rss_id'] in skip:
                        folder_name = hashlib.sha256(row['rss_url'].encode()).hexdigest()[:5]  # Last 5 Chars of url's Sha256
                        folder = 'Rss_files_v2/' + f"{folder_name}"
                        sys.stdout.write(f"\t Got name of last file in folder: {folder} \n")
                        old_data = get_last_modified_file_data(bucket_name=bucket, folder_name=folder)

                        sys.stdout.write(f"\t Received old data. \n")

                        try:
                            cond = compare_xml_files(old_data, data)
                        except:
                            cond = False

                        if cond:
                            sys.stdout.write("\t Same as old xml files, only a few date/time fields have changed. \n")

                            new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'],
                                       'md5_hash': row['md5_hash'], 'last_changed': row['last_changed'],
                                       'change_interval': row['change_interval'], 'epoch_counter': row['epoch_counter']}

                        else:
                            epoch = int(row['epoch_counter']) + 1
                            sys.stdout.write("\t Changing hash from {} to {}. \n".format(row['md5_hash'], file_hash))
                            logger.info("\t Changing hash from {} to {}. \n".format(row['md5_hash'], file_hash))

                            now = datetime.datetime.now()
                            if last_changed == "null":
                                last_changed = now
                            diff_time = now - last_changed
                            change_interval = str(diff_time).split(".")[0]

                            new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'], 'md5_hash': file_hash,
                                       'last_changed': str(now), 'change_interval': change_interval, 'epoch_counter': epoch}

                            folder_name = hashlib.sha256(row['rss_url'].encode()).hexdigest()[
                                          :5]  # Last 5 Chars of url's Sha256
                            file_name = f"{str(time.time())}.xml"
                            write_path = 'Rss_files_v2/' + f"{folder_name}/{file_name}"
                            temp_file_name = "temp_file.txt"

                            with open(temp_file_name, 'w') as new_file:
                                new_file.write(data)
                                sys.stdout.write("\t Writing to file: {}. \n".format(write_path))

                            # s3.Bucket(bucket).upload_file(temp_file_name, write_path)
                            s3.upload_file(Filename=temp_file_name, Bucket=bucket, Key=write_path)

                            sys.stdout.write("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))
                            logger.info("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))

                    else:
                        new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'],
                                   'md5_hash': row['md5_hash'], 'last_changed': row['last_changed'],
                                   'change_interval': row['change_interval'], 'epoch_counter': row['epoch_counter']}

                        sys.stdout.write("Skipping")

                writer.writerow(new_row)

            except Exception as e:
                sys.stdout.write("Skipping for Rss_ID: {} : Error: {}\n".format(row['rss_id'], e))
                logger.info("Skipping for Rss_ID: {} : Error: {}\n".format(row['rss_id'], e))
                new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'],
                           'md5_hash': row['md5_hash'], 'last_changed': row['last_changed'],
                           'change_interval': row['change_interval'], 'epoch_counter': row['epoch_counter']}
                writer.writerow(new_row)

    shutil.move(temp_file.name, filename)
    sys.stdout.write("Completed Going through all Rss feeds. \n")
    time.sleep(300)  # Sleep for 5 mins

# sudo ssh - i Rss_collector.pem ubuntu@18.183.76.127
# nohup python3 -u rss_collector.py > cmd.log &
# sudo ssh 65.2.11.21 -l ubuntu -i allwin-key-rss-feed.pem
# nohup python rss_collector.py &
# ps -ef | grep python
# kill <pid>
