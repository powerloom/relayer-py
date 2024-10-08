import json


class RPCException(Exception):
    """
    Custom exception class for RPC-related errors.

    This exception encapsulates information about an RPC request, its response,
    any underlying exception, and additional context.
    """

    def __init__(self, request, response, underlying_exception, extra_info):
        """
        Initialize the RPCException.

        Args:
            request: The original RPC request.
            response: The response received from the RPC call.
            underlying_exception (Exception): The original exception that was caught, if any.
            extra_info: Any additional context or information about the error.
        """
        self.request = request
        self.response = response
        self.underlying_exception: Exception = underlying_exception
        self.extra_info = extra_info

    def __str__(self):
        """
        Return a string representation of the exception.

        Returns:
            str: A JSON-formatted string containing all the exception details.
        """
        ret = {
            'request': self.request,
            'response': self.response,
            'extra_info': self.extra_info,
            'exception': None,
        }
        # Include the underlying exception's string representation if it exists
        if isinstance(self.underlying_exception, Exception):
            ret.update({'exception': str(self.underlying_exception)})
        return json.dumps(ret)

    def __repr__(self):
        """
        Return a string representation of the exception for debugging.

        Returns:
            str: The same string as __str__ method.
        """
        return self.__str__()
