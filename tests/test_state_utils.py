import unittest
import simplejson as json
from unittest import TestCase
from unittest.mock import patch
from botocore.exceptions import ClientError

import constants as c
from classes import Stream, Shard
from state_utils import load_state_from_db, save_state_to_db


class TestLoadStateFromDB(TestCase):
    @patch('state_utils.dynamodb')
    @patch('state_utils.logger')
    def test_load_state_from_db_success(self, mock_logger, mock_dynamodb):
        # Set up a mock response for the DynamoDB get_item call
        mock_response = {
            'Item': {
                'streams_state': {'S': json.dumps([{
                    'name': 'test_stream',
                    'shards': [{'id': 'shardId-00000000001', 'sequence_number': '12345', 'next_shard_iterator': '12345'}],
                    'subscribers': ['http://example.com']
                }])}
            }
        }
        mock_dynamodb.get_item.return_value = mock_response

        result = load_state_from_db()

        expected_stream = Stream(name='test_stream')

        self.assertEqual(result, {expected_stream}) # streams are equal if names are equal
        mock_logger.error.assert_not_called()

    @patch('state_utils.dynamodb')
    @patch('state_utils.logger')
    def test_load_state_from_db_empty_result(self, mock_logger, mock_dynamodb):
        mock_response = {}
        mock_dynamodb.get_item.return_value = mock_response

        result = load_state_from_db()

        self.assertEqual(result, set())
        mock_logger.error.assert_called_once()


    @patch('state_utils.dynamodb')
    @patch('state_utils.logger')
    def test_load_state_from_db_exception(self, mock_logger, mock_dynamodb):
        mock_dynamodb.get_item.side_effect = ClientError({"Error": {"Code": 500}}, "mock")

        load_state_from_db()

        mock_dynamodb.get_item.assert_called_once()
        mock_logger.error.assert_called_once()


class TestSaveStateToDB(TestCase):
    @patch('state_utils.dynamodb')
    @patch('state_utils.logger')
    def test_save_state_to_db_exception(self, mock_logger, mock_dynamodb):
        streams = {
            Stream(name='test_stream_1', shards=[Shard(id='shardId-001', sequence_number='12345', next_shard_iterator='ABCDE')], subscribers=['http://example.com'])
        }
        mock_dynamodb.put_item.side_effect = ClientError({"Error": {"Code": 500}}, "mock")

        save_state_to_db(streams)

        mock_dynamodb.put_item.assert_called_once()
        mock_logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()