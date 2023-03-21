#!/bin/bash

source /home/ubuntu/env/bin/activate
cd /home/ubuntu/Rss_collector
nohup python3 -u /home/ubuntu/Rss_collector/rss_collector.py > cmd.log &
