import unittest
from unittest import TestCase
from unittest.mock import patch
from botocore.exceptions import ClientError

from classes import Stream, Shard


class TestStreamGetRecord(TestCase):
    @patch("classes.kinesis")
    def test_get_record_with_next_shard_iterator(self, mock_kinesis):
        stream = Stream(name="test_stream")
        shard = Shard(id="shardId-001", next_shard_iterator="123")
        mock_kinesis.get_records.return_value = {"Records": [{"SequenceNumber": "123", "Data": b"'data5'", "PartitionKey": "123"}], "NextShardIterator": "AAAAAA", "MillisBehindLatest": 0}
        
        records_response = stream.get_record(shard)

        self.assertEqual(records_response, mock_kinesis.get_records.return_value)


    @patch("classes.kinesis")
    def test_get_record_with_without_shard_iterator(self, mock_kinesis):
        stream = Stream(name="test_stream")
        shard = Shard(id="shardId-00000000001", sequence_number="12345")
        mock_kinesis.get_records.return_value = {"Records": [{"SequenceNumber": "123", "Data": b"'data5'", "PartitionKey": "123"}], "NextShardIterator": "12345", "MillisBehindLatest": 0}
        mock_kinesis.get_shard_iterator.return_value = {"ShardIterator": "12345"}

        records_response = stream.get_record(shard)

        self.assertEqual(records_response, mock_kinesis.get_records.return_value)


class TestSaveStateToDB(TestCase):
    @patch("classes.dynamodb")
    def test_get_subscribers_success(self, mock_dynamodb):
        stream = Stream(name="test_stream_1", shards=[Shard(id="shardId-001", sequence_number="12345", next_shard_iterator="ABCDE")], subscribers=["http://example1.com"])
        mock_dynamodb.query.return_value = {"Items": [{"sk": {"S": "ID#1111"}, "url": {"S": "http://example2.com"}, "pk": {"S": "test1"}}]}

        stream.get_subscribers()

        self.assertEqual(stream.subscribers, ["http://example1.com", "http://example2.com"])


    @patch("classes.dynamodb")
    @patch("classes.logger")
    def test_get_subscribers_client_exception(self, mock_logger, mock_dynamodb):
        mock_dynamodb.query.side_effect = ClientError({"Error": {"Code": 500}}, "mock")
        stream = Stream(name="test_stream_1", shards=[Shard(id="shardId-001", sequence_number="12345", next_shard_iterator="ABCDE")], subscribers=["http://example1.com"])
    
        stream.get_subscribers()

        mock_dynamodb.query.assert_called_once()
        mock_logger.error.assert_called_once()


    @patch("classes.dynamodb")
    @patch("classes.logger")
    def test_get_subscribers_exception(self, mock_logger, mock_dynamodb):
        mock_dynamodb.query.side_effect = Exception("Test exception")
        stream = Stream(name="test_stream_1", shards=[Shard(id="shardId-001", sequence_number="12345", next_shard_iterator="ABCDE")], subscribers=["http://example1.com"])
        
        stream.get_subscribers()

        mock_dynamodb.query.assert_called_once()
        mock_logger.error.assert_called_once()





if __name__ == "__main__":
    unittest.main()