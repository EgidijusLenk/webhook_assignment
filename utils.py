import time
import logging
from typing import Tuple
from urllib3.util import Retry

import requests
import boto3
from requests.adapters import HTTPAdapter
from watchtower import CloudWatchLogHandler
from botocore.exceptions import ClientError

import constants as c


def get_logger():
    logger = logging.getLogger("Logger")
    logger.setLevel(logging.INFO)
    cw_handler = CloudWatchLogHandler(log_group_name=c.CLOUDWATCH_LOG_GROUP, log_stream_name=c.CLOUDWATCH_STREAM_NAME)
    logger.addHandler(cw_handler)
    return logger

logger = get_logger()


def get_aws_resources() -> Tuple[boto3.client, boto3.client]:
    while True:
        try:
            kinesis = boto3.client('kinesis')
            dynamodb = boto3.client('dynamodb', config=c.RETRY_CONFIG)
            return kinesis, dynamodb
        except ClientError as e:
            logger.error(f"Boto3 error: {e}. Retrying in 30 seconds")
            time.sleep(30)
        
kinesis, dynamodb = get_aws_resources()


def authenticate() -> str:
    return c.AUTH_TOKEN


def get_session() -> requests.Session:
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[401, 429, 500, 502, 503, 504],
        )
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session = requests.Session()
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    return session

session = get_session()


def retry_failed_request(endpoint: str, data: str, stream_name: str) -> None:
    try:
        session.post(url=endpoint, data=data)
    except Exception as e:
        logger.error(f"Failed to send data to subscriber {endpoint}. Error: {e}. Stream: {stream_name}")


def process_streams_subscribers(streams) -> None:
    for stream in streams:
        stream.get_subscribers()
