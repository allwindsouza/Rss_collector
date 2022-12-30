import csv
import requests
import hashlib
import time
import shutil
import sys
from tempfile import NamedTemporaryFile
import logging
import datetime

logging.basicConfig(filename="/home/ubuntu/rss_collector/logs.log",
                    format='%(asctime)s %(message)s',
                    filemode='w')

logger = logging.getLogger()
logger.setLevel(logging.INFO)

logger.info(f"Starting to Iterate over the rss URLS at {datetime.datetime.now()}")

filename = 'rss_urls.csv'
temp_file = NamedTemporaryFile(mode='w', delete=False)

fields = ['rss_id', 'rss_url', 'md5_hash']

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

            if hash == row['md5_hash']:
                sys.stdout.write("\t Same hash as previous iteration: {}. \n".format(hash))
                logger.info("\t Same hash as previous iteration: {}. \n".format(hash))
                new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'], 'md5_hash': row['md5_hash']}

            else:
                sys.stdout.write("\t Changing hash from {} to {}. \n".format(row['md5_hash'], hash))
                logger.info("\t Changing hash from {} to {}. \n".format(row['md5_hash'], hash))

                new_row = {'rss_id': row['rss_id'], 'rss_url': row['rss_url'], 'md5_hash': hash}
                file_name = row['rss_id'] + '_:_' + str(time.time())
                with open(file_name, 'w') as new_file:
                    new_file.write(data)
                    sys.stdout.write("\t Writing to file: {}. \n".format(file_name))
                sys.stdout.write("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))
                logger.info("\t Completed Processing Rss_ID: {} . \n".format(row['rss_id']))

            writer.writerow(new_row)
        except:
            sys.stdout.write("Skipping for Rss_ID: {} : \n".format(row['rss_id']))
            logger.info("Skipping for Rss_ID: {} : \n".format(row['rss_id']))

shutil.move(temp_file.name, filename)


