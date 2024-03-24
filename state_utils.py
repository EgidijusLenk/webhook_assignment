from typing import Set
import simplejson as json
from botocore.exceptions import ClientError

from utils import logger, dynamodb
from classes import Stream, Shard
import constants as c


def load_state_from_db() -> Set[Stream]:
    try:
        response = dynamodb.get_item(
        TableName=c.DDB_TABLE_NAME,
        Key={
            'pk': {'S': c.DDB_STREAMS_STATE_K},
            'sk': {'S': c.DDB_STREAMS_STATE_K}
        }
        )

        item = response.get('Item')
        if item:
            streams_state_str = item.get('streams_state', {}).get('S')
            if streams_state_str:
                streams_state = json.loads(streams_state_str)
                streams = {Stream(name=stream['name'],
                                  shards=[Shard(**shard) for shard in stream['shards']],
                                  subscribers=stream['subscribers'])
                           for stream in streams_state}
            return streams
        else:
            logger.error(f"Failed to pasrse Streams state from db.")   
            return set() 
    except ClientError as e:
        logger.error(f"Boto3 error while reading from Streams state: {e}.")   
        return set() 
    

def save_state_to_db(streams: Set[Stream]) -> None:
    try:
        streams_state = json.dumps([stream.to_dict() for stream in streams])
        item = {
            'pk': {'S': c.DDB_STREAMS_STATE_K},
            'sk': {'S': c.DDB_STREAMS_STATE_K},
            'streams_state': {'S': streams_state}
        }

        dynamodb.put_item(
            TableName='webhooks_ddb_table',
            Item=item
        )
    except ClientError as e:
        logger.error(f"Error while writing Streams state to db: {e}")


# load_state_from_file and save_state_to_file could be used as fallback option if retrieving state from ddb fails
def load_state_from_file() -> Set[Stream]:
    streams: Set[Stream] = set()
    try:
        with open('streams.json', 'r') as file:
            streams_state = json.load(file)
        streams = {Stream(name=stream['name'],
                        shards=[Shard(**shard) for shard in stream['shards']],
                        subscribers=stream['subscribers'])
                    for stream in streams_state}
    except Exception as e:
        logger.error(f"Error while reading from Streams state: {e}")
    return streams


def save_state_to_file(streams: Set[Stream]) -> None:
    try:
        with open('streams.json', 'w') as file:
            streams_state = [stream.to_dict() for stream in streams]
            json.dump(streams_state, file)
    except Exception as e:
        logger.error(f"Error while writing Streams state: {e}")

