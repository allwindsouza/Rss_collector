import csv
import requests
import hashlib
import time
import shutil
import sys
from tempfile import NamedTemporaryFile
import logging
import datetime
import boto3
import creds

logging.basicConfig(
                    filename='logs.log',
                    format='%(asctime)s %(message)s',
                    filemode='w'
                    )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3', aws_access_key_id=creds.aws_access_key_id,
                    aws_secret_access_key=creds.aws_secret_access_key,
                    aws_session_token=creds.aws_session_token)

bucket = 'pub-rss-feed-collection'

logger.info(f"Starting to Iterate over the rss URLS at {datetime.datetime.now()}. \n")
sys.stdout.write(f"Starting to Iterate over the rss URLS at {datetime.datetime.now()}. \n")

filename = 'temp_rss_feed.csv'

fields = ['rss_id', 'rss_url', 'md5_hash', 'last_changed', 'change_interval']

while True:
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
                hash = hashlib.md5(data.encode('utf-8')).hexdigest()

                last_changed = row['last_changed']
                if last_changed != "null":
                    last_changed = datetime.datetime.strptime(row['last_changed'], '%Y-%m-%d %H:%M:%S.%f')


                if hash == row['md5_hash']:
                    sys.stdout.write("\t Same hash as previous iteration: {}. \n".format(hash))
                    logger.info("\t Same hash as previous iteration: {}. \n".format(hash))
                    new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'],
                               'md5_hash': row['md5_hash'], 'last_changed': row['last_changed'],
                               'change_interval': row['change_interval']}

                else:
                    sys.stdout.write("\t Changing hash from {} to {}. \n".format(row['md5_hash'], hash))
                    logger.info("\t Changing hash from {} to {}. \n".format(row['md5_hash'], hash))

                    now = datetime.datetime.now()
                    if last_changed == "null":
                        last_changed = now
                    diff_time = now - last_changed
                    change_interval = str(diff_time).split(".")[0]

                    new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'], 'md5_hash': hash,
                               'last_changed': str(now), 'change_interval': change_interval}
                    file_name = row['rss_id'] + '_' + str(time.time())

                    temp_file_name = "temp_file.txt"
                    with open(temp_file_name, 'w') as new_file:
                        new_file.write(data)
                        sys.stdout.write("\t Writing to file: {}. \n".format(file_name))

                    print("Uploading to bucket")
                    s3.Bucket(bucket).upload_file(temp_file_name, 'Rss_files/' + file_name)

                    sys.stdout.write("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))
                    logger.info("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))

                writer.writerow(new_row)
            except:
                sys.stdout.write("Skipping for Rss_ID: {} : \n".format(row['rss_id']))
                logger.info("Skipping for Rss_ID: {} : \n".format(row['rss_id']))

    shutil.move(temp_file.name, filename)
    sys.stdout.write("Completed Going through all Rss feeds")
    time.sleep(300) # Sleep for 5 mins

# sudo ssh - i Rss_collector.pem ubuntu@18.183.76.127
# sudo ssh 65.2.11.21 -l ubuntu -i allwin-key-rss-feed.pem
# nohup python rss_collector.py &
# ps -ef | grep python
# kill <pid>
