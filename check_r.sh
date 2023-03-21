#!/bin/bash


# Define the command to check if the RSS server is running
RSS_SERVER_CMD="python3 -u /home/ubuntu/Rss_collector/rss_collector.py"
RSS_SERVER_CMD_START="./collect.sh"

# Define the interval between checks (in seconds)
CHECK_INTERVAL=180

while true; do
    # Check if the RSS server is running
    if pgrep -x "$(basename $RSS_SERVER_CMD)" >/dev/null; then
        echo "RSS collector is running"
    else
        echo "RSS collector is not running, starting it now..."
        # Start the RSS server if it's not already running
        $RSS_SERVER_CMD_START &
    fi
    # Wait for the next check interval
    sleep $CHECK_INTERVAL
done

