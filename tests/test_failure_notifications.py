from datetime import datetime
from data_models import RelayerIssue



import asyncio

from utils.notification_utils import send_failure_notifications

# poetry run python3 -m tests.test_failure_notifications

class MockNonceFailureException(Exception):
    def __init__(self, message="Nonce too low", *args):
        super().__init__(message, *args)


class MockAsyncClient:
    def __init__(self, response=None, raise_exception=False):
        """
        Initialize the mock client.
        :param response: The mock response to return from post requests.
        :param raise_exception: If True, raise an exception on post requests.
        """
        self.response = response
        self.raise_exception = raise_exception

    async def post(self, url, json):
        """
        Simulate an asynchronous post request.
        :param url: The URL to post to.
        :param json: The JSON data to send.
        """
        print(f"Mock post request to {url} with data: {json}")
        if self.raise_exception:
            raise Exception("Mock exception from post request")
        return self.response

    async def close(self):
        """ Simulate closing the client. """
        print("Mock client closed")


async def test_send_failure_notifications():
    # Mock client without exception
    mock_client = MockAsyncClient(response="Success")

    mock_exception = MockNonceFailureException()

    mock_message = RelayerIssue(
        timeOfReporting=datetime.now().isoformat(), 
        issueType='relayer failed to submit snapshot',
        extra=str(mock_exception)
    )

    await send_failure_notifications(mock_client, mock_message)

    # Mock client with exception in post request. displays traceback
    mock_client_with_exception = MockAsyncClient(raise_exception=True)
    await send_failure_notifications(mock_client_with_exception, mock_message)

# Replace `your_message_model` with an instance of your BaseModel

if __name__ == "__main__":
    asyncio.run(test_send_failure_notifications())
    