import resource

from gunicorn.app.base import BaseApplication

from relayer import app
from settings.conf import settings


def post_worker_init(worker):
    """
    Initialize worker settings after the worker process has been created.

    This function sets the maximum number of open file descriptors for the worker process.

    Args:
        worker: The Gunicorn worker instance.
    """
    # Get the current soft and hard limits for file descriptors
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)

    # Set new file descriptor limits based on settings
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (settings.rlimit.file_descriptors, hard),
    )


class StandaloneApplication(BaseApplication):
    """
    Custom Gunicorn application class for standalone deployment.

    This class extends Gunicorn's BaseApplication to provide a customized
    WSGI application setup.
    """

    def __init__(self, app, options=None):
        """
        Initialize the StandaloneApplication.

        Args:
            app: The WSGI application to run.
            options (dict, optional): Configuration options for Gunicorn.
        """
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        """
        Load the Gunicorn configuration from the provided options.

        This method filters and sets the configuration options for Gunicorn.
        """
        config = {
            key: value for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        """
        Load and return the WSGI application.

        Returns:
            The WSGI application to be run by Gunicorn.
        """
        return self.application


if __name__ == '__main__':
    # Define Gunicorn server options
    options = {
        'bind': f'{settings.relayer_service.host}:{settings.relayer_service.port}',
        'keepalive': settings.relayer_service.keepalive_secs,
        'workers': len(settings.signers),
        'timeout': 120,
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'post_worker_init': post_worker_init,
    }

    # Run the standalone Gunicorn application
    StandaloneApplication(app, options).run()
