"""
This module configures and sets up a custom logger using the Loguru library.

It defines a custom log format and configures log handlers for different output streams
and log levels.
"""
import sys

from loguru import logger

# Define a custom log format
# {extra} field can be used to pass extra parameters to the logger using .bind()
FORMAT = '{time:MMMM D, YYYY > HH:mm:ss!UTC} | {level} | {message}| {extra}'

# Remove the default handler
logger.remove(0)

# Add custom handlers for different output streams and log levels
# Add a handler for stdout to capture DEBUG and INFO level logs
logger.add(sys.stdout, level='DEBUG', format=FORMAT)

# Add a handler for stderr to capture WARNING level logs
logger.add(sys.stderr, level='WARNING', format=FORMAT)

# Add another handler for stderr to capture ERROR and CRITICAL level logs
logger.add(sys.stderr, level='ERROR', format=FORMAT)
