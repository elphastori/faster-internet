import logging
import json
import re
import subprocess
import datetime
import os

import boto3

STREAM_NAME = "PingStream"
REGION = "us-east-1"

logger = logging.getLogger(__name__)
kinesis_client = boto3.client('kinesis', region_name=REGION)

def ping_url(url):
    response = subprocess.Popen(
        f"/sbin/ping -c 1 {url}",
        shell=True,
        stdout=subprocess.PIPE).stdout.read().decode('utf-8')

    return float(re.search('time=(.*?)\s', response, re.MULTILINE).group(1))

def ping_urls():
    logging.info(f"Pinging URLs...")

    google = ping_url("google.com")
    facebook = ping_url("facebook.com")
    amazon = ping_url("amazon.com")

    time = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"

    return ({
        "time": time,
        "google": google,
        "facebook": facebook,
        "amazon": amazon,
        "host": os.environ["HOST"]
    })

def put_results(ping_stats):
    logging.info(f"Putting results to Kinesis: {ping_stats}")

    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(ping_stats),
        PartitionKey="partitionkey")

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    put_results(ping_urls())