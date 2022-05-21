import logging
import json
import re
import subprocess
import datetime
import os

import boto3

STREAM_NAME = "SpeedtestStream"
REGION = "us-east-1"

logger = logging.getLogger(__name__)
kinesis_client = boto3.client('kinesis', region_name=REGION)


def run_speedtest():
    logging.info(f"Running speedtest...")

    response = subprocess.Popen(
        '/usr/bin/speedtest --accept-license --accept-gdpr',
        shell=True,
        stdout=subprocess.PIPE).stdout.read().decode('utf-8')

    time = datetime.datetime.utcnow().isoformat(timespec="milliseconds") + "Z"
    ping = re.search('Latency:\s+(.*?)\s', response, re.MULTILINE).group(1)
    download = re.search('Download:\s+(.*?)\s', response, re.MULTILINE).group(1)
    upload = re.search('Upload:\s+(.*?)\s', response, re.MULTILINE).group(1)
    jitter = re.search('\((.*?)\s.+jitter\)\s', response, re.MULTILINE).group(1)

    return ({
        "time": time,
        "ping": float(ping),
        "jitter": float(jitter),
        "download": float(download),
        "upload": float(upload),
        "host": os.environ["HOST"]
    })


def put_results(speedtest):
    logging.info(f"Putting results to Kinesis: {speedtest}")

    kinesis_client.put_record(
        StreamName=STREAM_NAME,
        Data=json.dumps(speedtest),
        PartitionKey="partitionkey")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")

    speedtest = run_speedtest()

    put_results(speedtest)
