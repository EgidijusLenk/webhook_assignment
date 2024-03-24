import asyncio
import threading
from typing import List
from dataclasses import dataclass, asdict, field

import aiohttp
from botocore.exceptions import ClientError

import constants as c
from utils import retry_failed_request, dynamodb, kinesis, logger


@dataclass
class Shard:
    id: str = ""
    sequence_number: str = ""
    next_shard_iterator: str = ""

    def __hash__(self):
        return hash(self.id)

    def __eq__(self, other):
        if isinstance(other, Shard):
            return self.id == other.id
        return NotImplemented

    def to_dict(self) -> dict:
        return asdict(self)
    

@dataclass
class Stream:
    name: str = ""
    shards: List[Shard] = field(default_factory=list)
    subscribers: List[str] = field(default_factory=list)


    def __hash__(self):
        return hash(self.name)


    def __eq__(self, other):
        if isinstance(other, Stream):
            return self.name == other.name
        return NotImplemented


    def to_dict(self) -> dict:
        return {
            "name": self.name,
            "shards": [shard.to_dict() for shard in self.shards],
            "subscribers": self.subscribers
        }
    

    def update_shards(self, response_data: dict) -> None:
        shards = [Shard(id=shard["ShardId"]) for shard in response_data.get("StreamDescription", {}).get("Shards", {})]
        self.shards.extend([shard for shard in shards if shard not in self.shards])


    def get_records(self) -> List[str]:
        records = []
        for shard in self.shards:
            records_response = self.get_record(shard)
            if "Records" in records_response and records_response["Records"]:
                last_sequence_number = records_response["Records"][-1]["SequenceNumber"]
                shard.sequence_number = last_sequence_number
                records.extend([record["Data"] for record in records_response["Records"]])
            shard.next_shard_iterator = records_response.get("NextShardIterator", {})
        return records


    def get_record(self, shard) -> dict:
        records_response = {}

        if shard.next_shard_iterator:
            shard_iterator = shard.next_shard_iterator
        else:
            shard_iterator_params = {
                "StreamName": self.name,
                "ShardId": shard.id,
                "ShardIteratorType": "TRIM_HORIZON"
                }
            
            if shard.sequence_number:
                shard_iterator_params["StartingSequenceNumber"] = shard.sequence_number
                shard_iterator_params["ShardIteratorType"] = "AFTER_SEQUENCE_NUMBER"

            try:
                shard_iterator = kinesis.get_shard_iterator(**shard_iterator_params)["ShardIterator"]
            except ClientError as e:
                logger.error(f"Boto3 error occured while getting shard iterator for {shard} in stream {self.name}: {e} ")
                if "ResourceNotFoundException" in str(e) and shard.id in str(e) and shard.id in self.shards:
                    self.shards.remove(shard.id)
                return records_response
            
        try:
            records_response = kinesis.get_records(
                ShardIterator=shard_iterator,
                Limit=100
                )
        except ClientError as e:
            if e.response["Error"]["Code"] in {"ExpiredIteratorException", "InvalidArgumentException"}:
                # fallback option to retrieve records after last sequence_number:
                shard.next_shard_iterator = ""
                records_response = self.get_record(shard)
            else:
                logger.error(f"Error occured in get_record: {e} ")
        return records_response


    async def send_data_to_subscribers(self, data) -> None:
        print(data) #TODO remove print in pord
        if data and self.subscribers:
            async with aiohttp.ClientSession() as session:
                tasks = [self.send_data_to_subscriber(session, subscriber, data) for subscriber in self.subscribers]
                await asyncio.gather(*tasks)


    async def send_data_to_subscriber(self, session, subscriber, data) -> None:
        try:
            async with session.post(subscriber, data=data) as response:

                if response.status != 200:
                    logger.warning(f"Stream: {self.name}. {subscriber} responded with {response.status}, retrying...")
                    t = threading.Thread(target=retry_failed_request, args=(subscriber, data, self.name))
                    t.start()
        except Exception as e:
            logger.error(f"Error while sending data to subscriber {subscriber}. Error: {e}. Stream: {self.name}")

    def get_subscribers(self) -> None:
        try:
            response = dynamodb.query(
                TableName=c.DDB_TABLE_NAME,
                KeyConditionExpression="pk = :pk_val",
                ExpressionAttributeValues={
                    ":pk_val": {"S": self.name}
                }
            )
            for item in response.get("Items", []):
                url = item.get("url", {}).get("S")
                if url and url not in self.subscribers:
                    self.subscribers.append(url)
        except ClientError as e:
            logger.error(f"Boto3 error while getting subscribers for Stream {self.name}: {e}.")
        except Exception as e:
            logger.error(f"Error while getting subscribers for Stream {self.name}: {e}.")