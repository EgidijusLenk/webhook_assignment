import os
from dotenv import load_dotenv
from botocore.config import Config

load_dotenv()

API_BASE_URL = os.getenv("API_BASE_URL")
AUTH_TOKEN = os.getenv("AUTH_TOKEN")
DDB_TABLE_NAME = os.getenv("DDB_TABLE_NAME")
DDB_STREAMS_STATE_K = os.getenv("DDB_STREAMS_STATE_K")
CLOUDWATCH_LOG_GROUP = os.getenv("CLOUDWATCH_LOG_GROUP")
CLOUDWATCH_STREAM_NAME = os.getenv("CLOUDWATCH_STREAM_NAME")

RETRY_CONFIG = Config(
    retries={
        'max_attempts': int(os.getenv("MAX_ATTEMPTS")),
        'mode': os.getenv("RETRY_MODE")
    }
)