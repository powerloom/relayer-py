import asyncio

from httpx import AsyncClient
from pydantic import BaseModel

from settings.conf import settings
from utils.default_logger import logger


# Bind logger with module information
logger = logger.bind(
    module='PowerLoom|OnChainConsensus|NotificationUtils',
)


def misc_notification_callback_result_handler(fut: asyncio.Future):
    """
    Handle the result of an asynchronous notification or callback operation.

    This function is used as a callback for asyncio.Future objects. It logs the result
    or any exceptions that occurred during the operation.

    Args:
        fut (asyncio.Future): The future object representing the completed operation.
    """
    try:
        r = fut.result()
    except Exception as e:
        logger.opt(exception=True).error(
            'Exception while sending callback or notification: {}', e,
        )
    else:
        logger.debug('Callback or notification result: {}', r)


async def send_reporting(client: AsyncClient, message: BaseModel):
    try:
        resp = await client.post(
            url=settings.reporting.service_url,
            json=message.dict(),
        )
        logger.debug(
            'Reporting service notification sent: {}',
            resp.status_code,
        )
    except Exception as e:
        logger.opt(exception=True).error(
            'Exception while sending reporting service notification: {}', e,
        )


async def send_slack(client: AsyncClient, message: BaseModel):
    try:
        resp = await client.post(
            url=settings.reporting.slack_url,
            json=message.dict(),
        )
        logger.debug('Slack notification sent: {}', resp.status_code)
    except Exception as e:
        logger.opt(exception=True).error(
            'Exception while sending Slack notification: {}', e,
        )


async def send_failure_notifications(client: AsyncClient, message: BaseModel):
    """
    Send failure notifications to configured services.

    This function attempts to send notifications to a reporting service and/or Slack
    if their respective URLs are configured in the settings.

    Args:
        client (AsyncClient): An HTTP client for making asynchronous requests.
        message (BaseModel): A Pydantic model containing the notification message.

    Note:
        This function does not wait for the notifications to complete. It schedules
        them as background tasks and attaches a callback to handle their results.
    """
    # Send notification to reporting service if URL is configured
    if settings.reporting.service_url:
        fut = asyncio.ensure_future(send_reporting(client, message))
        fut.add_done_callback(misc_notification_callback_result_handler)

    # Send notification to Slack if URL is configured
    if settings.reporting.slack_url:
        fut = asyncio.ensure_future(send_slack(client, message))
        fut.add_done_callback(misc_notification_callback_result_handler)
