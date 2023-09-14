import json
import resource

from gunicorn.app.base import BaseApplication

from relayer import app
from settings.conf import settings


# reset pid.json
with open('pid.json', 'w') as pid_file:
    json.dump([], pid_file)


def post_worker_init(worker):
    # add worker pid to a list and store that file

    with open('pid.json', 'r') as pid_file:
        data = json.load(pid_file)
        data.append(worker.pid)

    with open('pid.json', 'w') as pid_file:
        json.dump(data, pid_file)

    # print(worker.app.application.state.worker_id)
    soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
    resource.setrlimit(
        resource.RLIMIT_NOFILE,
        (settings.rlimit.file_descriptors, hard),
    )


class StandaloneApplication(BaseApplication):
    """Our Gunicorn application."""

    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        config = {
            key: value for key, value in self.options.items()
            if key in self.cfg.settings and value is not None
        }
        for key, value in config.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application


if __name__ == '__main__':

    options = {
        'bind': f'{settings.relayer_service.host}:{9030}',
        'keepalive': settings.relayer_service.keepalive_secs,
        'workers': len(settings.signers),
        'timeout': 120,
        'worker_class': 'uvicorn.workers.UvicornWorker',
        'post_worker_init': post_worker_init,
    }

    StandaloneApplication(app, options).run()
