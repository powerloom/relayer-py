class SelfExitException(Exception):
    """
    Exception raised to signal the process hub core to exit.

    This exception is used internally by the process hub core to initiate
    a graceful shutdown of the core process.
    """
    pass


class GenericExitOnSignal(Exception):
    """
    Exception raised when a process receives a signal to exit.

    This exception is used by launched processes or callback workers
    when they receive a signal to terminate (e.g., SIGINT, SIGTERM, SIGQUIT).
    It allows for graceful shutdown and cleanup of resources.
    """
    pass
