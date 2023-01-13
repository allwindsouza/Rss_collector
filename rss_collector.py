import csv
import requests
import time
import shutil
import sys
from tempfile import NamedTemporaryFile
import logging
import datetime
import boto3
import hashlib
import configparser

config = configparser.ConfigParser()
config.read('config.ini')

logging.basicConfig(
                    filename='logs.log',
                    format='%(asctime)s %(message)s',
                    filemode='w'
                    )

logger = logging.getLogger()
logger.setLevel(logging.INFO)

s3 = boto3.resource('s3', aws_access_key_id=config['503037447114_aws-compass-user1']['aws_access_key_id'],
                    aws_secret_access_key=config['503037447114_aws-compass-user1']['aws_secret_access_key'],
                    aws_session_token=config['503037447114_aws-compass-user1']['aws_session_token'])

bucket = 'pub-rss-feed-collection'

filename = 'temp_rss_feed.csv'

fields = ['rss_id', 'rss_url', 'md5_hash', 'last_changed', 'change_interval', 'epoch_counter']

while True:

    logger.info(f"Starting to Iterate over the rss URLS at {datetime.datetime.now()}. \n")
    sys.stdout.write(f"Starting to Iterate over the rss URLS at {datetime.datetime.now()}. \n")

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
                    epoch = int(row['epoch_counter']) + 1
                    sys.stdout.write("\t Changing hash from {} to {}. \n".format(row['md5_hash'], file_hash))
                    logger.info("\t Changing hash from {} to {}. \n".format(row['md5_hash'], file_hash))

                    now = datetime.datetime.now()
                    if last_changed == "null":
                        last_changed = now
                    diff_time = now - last_changed
                    change_interval = str(diff_time).split(".")[0]

                    new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'], 'md5_hash': file_hash,
                               'last_changed': str(now), 'change_interval': change_interval, 'epoch_counter': epoch }

                    folder_name = hashlib.sha256(row['rss_url'].encode()).hexdigest()[:5] # Last 5 Chars of url's Sha256
                    file_name = f"{str(time.time())}.xml"
                    write_path = 'Rss_files_v2/' + f"{folder_name}/{file_name}"
                    temp_file_name = "temp_file.txt"

                    with open(temp_file_name, 'w') as new_file:
                        new_file.write(data)
                        sys.stdout.write("\t Writing to file: {}. \n".format(write_path))

                    s3.Bucket(bucket).upload_file(temp_file_name, write_path)

                    sys.stdout.write("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))
                    logger.info("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))

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
    time.sleep(300) # Sleep for 5 mins

# sudo ssh - i Rss_collector.pem ubuntu@18.183.76.127
# sudo ssh 65.2.11.21 -l ubuntu -i allwin-key-rss-feed.pem
# nohup python rss_collector.py &
# ps -ef | grep python
# kill <pid>
