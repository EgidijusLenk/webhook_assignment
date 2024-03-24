import unittest
from unittest import TestCase
from unittest.mock import patch, Mock
from botocore.exceptions import ClientError

from utils import get_aws_resources, retry_failed_request


class TestGetAwsResources(TestCase):
    @patch("utils.boto3.client")
    @patch("utils.logger")
    @patch("utils.time.sleep")
    def test_get_streams_exception(self, mock_sleep, mock_logger, mock_boto3_client):
        mock_boto3_client.side_effect = [ClientError({"Error": {"Code": 500}}, "mock"), Mock(), Mock()]
        
        get_aws_resources()

        mock_sleep.assert_called_once_with(30)
        mock_logger.error.assert_called_once()


class TestRetryFailedRequest(TestCase):
    @patch("utils.session")
    @patch("utils.logger")
    def test_retry_failed_request_exception(self, mock_logger, mock_session):
        side_effect = Exception("Mocked exception")
        mock_session.post.side_effect = side_effect
        retry_failed_request(Mock(), Mock(), Mock())

        mock_logger.error.assert_called_once()


if __name__ == "__main__":
    unittest.main()