import unittest
from unittest import TestCase
from unittest.mock import patch, Mock

from main import get_streams, setup
from classes import Stream, Shard
import constants as c


class TestGetStreams(TestCase):
    @patch("main.session")
    @patch("main.logger")
    @patch("main.time.sleep")
    def test_get_streams_success(self, mock_sleep, mock_logger, mock_session):
        existing_streams = {Stream(name="ExistingStream1", shards=[], subscribers=["http://example2.com"]), Stream(name="ExistingStream2"), Stream(name="ExistingStream3")}
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"StreamNames": ["ExistingStream1", "Stream2"]}

        mock_session.get.return_value = mock_response
        streams = get_streams(existing_streams)
        
        self.assertEqual(len(streams), 2) # one stream had same name
        self.assertTrue(any(stream.name == "Stream2" for stream in streams))
        self.assertTrue(any(stream.name == "ExistingStream1" for stream in streams))
        mock_session.get.assert_called_once_with(c.API_BASE_URL)
        mock_logger.error.assert_not_called()
        mock_sleep.assert_not_called()


    @patch("main.session")
    @patch("main.logger")
    @patch("main.time.sleep")
    def test_get_streams_exception_and_retry(self, mock_sleep, mock_logger, mock_session):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"StreamNames": ["Stream1", "Stream2"]}
        side_effect = [Exception("Mocked exception1"), mock_response]
        mock_session.get.side_effect = side_effect

        streams = get_streams(set())

        # self.assertEqual(len(streams), 2)
        mock_logger.error.assert_called_once()
        mock_sleep.assert_called_once_with(30)


class TestRunSetup(TestCase):
    @patch("main.session")
    @patch("main.get_streams")
    def test_run_setup_success(self, mock_get_streams, mock_session):
        mock_streams = {Stream(name="Stream1"), Stream(name="Stream2")}
        mock_get_streams.return_value = mock_streams
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"StreamDescription": {"ShardId": "001"}}
        mock_session.get.return_value = mock_response

        result = setup(mock_streams)

        self.assertEqual(len(result), 2)


    @patch("main.session")
    @patch("main.logger")
    @patch("main.get_streams")
    def test_run_setup_exception_and_stream_removed(self, mock_get_streams, mock_logger, mock_session):
        mock_streams = {Stream(name="Stream1"), Stream(name="Stream2")}
        mock_get_streams.return_value = mock_streams
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"StreamDescription": {"ShardId": "001"}}
        mock_session.get.return_value = mock_response
        side_effect = [Exception("Mocked exception"), mock_response]
        mock_session.get.side_effect = side_effect

        result = setup(mock_streams)

        self.assertEqual(len(result), 1)
        mock_logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()