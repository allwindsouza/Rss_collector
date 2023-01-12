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

s3 = boto3.client('s3', aws_access_key_id=config['503037447114_aws-compass-user1']['aws_access_key_id'],
                    aws_secret_access_key=config['503037447114_aws-compass-user1']['aws_secret_access_key'],
                    aws_session_token=config['503037447114_aws-compass-user1']['aws_session_token'])

bucket = 'pub-rss-feed-collection'

rss_dict = {'1': 'https://feed.mp.lura.live/v3/feed/LADWMVDWFVWVZBZCAAFB/?fmt=mrss', '2': 'https://amagi-provisioner.0pb.org/rss/channels/id/ABiAj6xLz0IAZlr4@97:4017?apikey=262d3090-9ba0-44d0-81e4-3a70a01327f1', '3': 'http://feeds.cnevids.com/partner/amagi/bonappetit.mrss', '4': 'https://www.etonline.com/partners/video/cwdailynews.xml', '5': 'https://rss.zype.com/xumo/5ec56b8ecb6a4900015f2d27?app_key=t6bAdVijanfK7oT6xUdsEM6c6-6-C99MBDv_bjMqBk5HSQQePGR5e6JsIu6V1OoN', '7': 'https://feed.bluevo.com/BluCloud/JUNO/mrss/?id=P8EB9B71C697F499483B495D9D55ABC2B0958915AE8F94ED0BD1', '8': 'http://18.204.172.241/amagi-feed/', '9': 'https://www.popsugar.com/api/partners?partner=amagi&sites=fitsugar', '10': 'https://video-2.hatchfarmstudios.com/feeds/ac-redbox-e020d99b.xml', '11': 'https://ovp-vod-amagi-mrssfeeds-886239521314.s3.amazonaws.com/a6b51ef2-a7a7-44da-b870-c2d3910b10f2/mrssfeed.xml', '12': 'https://sensical-admin-production.herokuapp.com/api/feed/2-4', '13': 'http://tm-feed2-prod.tastemade.com.s3.amazonaws.com/amagi/amagi.xml', '14': 'http://tm-feed2-prod.tastemade.com.s3.amazonaws.com/amagi/amagi-pt.xml', '15': 'https://api.watch4.com/mrss?id=24&token=7284b369399b182396cdf2ad4bd275e7', '16': 'https://api.watch4.com/mrss?id=218&token=7284b369399b182396cdf2ad4bd275e7', '17': 'https://api.watch4.com/mrss?id=153&token=7284b369399b182396cdf2ad4bd275e7', '18': 'https://api.watch4.com/mrss?id=217&token=7284b369399b182396cdf2ad4bd275e7', '19': 'https://ovp-vod-amagi-mrssfeeds-886239521314.s3.amazonaws.com/4aa12bb3-7ddf-4261-8e89-80761c22289b/mrssfeed.xml', '20': 'https://cdn.jwplayer.com/v2/playlists/m33FrYs6?format=mrss', '21': 'https://pac-12.com/fd22ffeeaa13815573bc832658e939f4', '22': 'https://mollusk.apis.ign.com/rss/amagi?appKey=4fcda8ed-ee88-4b4d-8bb1-85975d6fc553', '23': 'https://feeds.esg.technology/amagi-latest/685.xml?Expires=1893456000&Signature=i-z0sx~yUa1lUOs3u0s7C4LFmPeuTlahhEIjUPINRhdVqJqclRoiZdtZTZvX8AzrPKWUG4iNwFGMor9jv1UVUvbdk~SVgIQEoNP4aoLOGQWS2fwp9O5p0JL7HnS8UpjlADUIInPxBQrXotyccdydMsBb0lHufLWKF0z7mjz1jVLRykEMEMvZqwnZhPfqIxywMaqnTF4UvjP5gx8wP53H9pX~WQvtgmAQQDKQkMuVo~smUX1L9Um9~1rYUqN5PMjst2vNnr3rXjxjIjh2xjIysnx9-XFv-U~B92GyQJ3Mp2j7x41XLliTE~roobRBPwlHJbyYwi7X5H7uQ0V~LcOtig__&Key-Pair-Id=APKAI4GLFMUGCZ7S4ECQ', '24': 'https://cdn.jwplayer.com/v2/playlists/aP6noDiH?format=mrss', '25': 'https://feed.theplatform.com/f/kYEXFC/X30aLQWplOWm', '26': 'https://ovp-vod-amagi-mrssfeeds-886239521314.s3.amazonaws.com/a5242359-31bf-47e4-bc77-78c10b002d33/mrssfeed.xml', '27': 'https://amagi-provisioner.0pb.org/rss/channels/id/ABducTvMHh0AOTbC@97:4017?apikey=262d3090-9ba0-44d0-81e4-3a70a01327f1', '28': 'https://syndication.crackle.com/feeds/linear/offnetworklinear_crackle_120.mrss?token=', '29': 'http://feeds.cnevids.com/partner/amagi/epicurious.mrss', '30': 'https://amagi-provisioner.0pb.org/rss/channels/id/ABgwdyi3P4oAM1HP@97:4017?apikey=262d3090-9ba0-44d0-81e4-3a70a01327f1', '31': 'http://tm-feed2-prod.tastemade.com.s3.amazonaws.com/amagi/amagi-es.xml', '32': 'https://ovp-vod-amagi-mrssfeeds-886239521314.s3.amazonaws.com/07fccd98-e917-4ffe-bd5b-d3c2000eaa47/mrssfeed.xml'}
epoch_counter_dict = {}

objects = s3.list_objects(Bucket=bucket)


for obj in objects['Contents']:
    # print(obj)
    # Get the file name
    file_path = obj['Key']
    old_file_name = str(file_path).split('/')[1]
    rss_id = old_file_name[0]
    folder_name = hashlib.sha256(rss_dict[rss_id].encode()).hexdigest()[:5]  # Last 5 Chars of url's Sha256
    if folder_name not in epoch_counter_dict:
        epoch_counter_dict[folder_name] = 0

    epoch_counter_dict[folder_name] += 1
    # print(old_file_name)
    new_file_name = f"{folder_name}/epoch_0{epoch_counter_dict[folder_name]}"

    old_file_path = f"Rss_files/{old_file_name}"
    destination = f"Rss_files_v2/{new_file_name}"

    # print(destination)
    # Move the file to the destination folder
    # s3.copy_object(Bucket=bucket, CopySource={'Bucket': bucket, 'Key': old_file_path}, Key=destination)
    # print("Done")
    # s3.delete_object(Bucket=bucket, Key=file_name)

