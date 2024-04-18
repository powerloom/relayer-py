import json
import resource
import threading
import time
import uuid
from multiprocessing import Process
from signal import SIGCHLD
from signal import SIGINT
from signal import signal
from signal import SIGQUIT
from signal import SIGTERM
from threading import Thread
from typing import Dict
from typing import Optional

import psutil
import redis

from data_models import ProcessorWorkerDetails
from helpers.redis_keys import tx_launcher_core_start_timestamp
from settings.conf import settings
from tx_checker import TxChecker
from tx_worker import TxWorker
from utils.default_logger import logger
from utils.helpers import cleanup_proc_hub_children
from utils.redis_conn import provide_redis_conn_repsawning_thread

REDIS_CONN_CONF = {
    'host': settings.redis.host,
    'port': settings.redis.port,
    'password': settings.redis.password,
    'db': settings.redis.db,
    'retry_on_error': [redis.exceptions.ReadOnlyError],
}


class TxWorkerLauncherCore(Process):
    _redis_connection_pool_sync: redis.BlockingConnectionPool
    _redis_conn_sync: redis.Redis

    def __init__(self, name, **kwargs):
        """
        Initializes a new instance of the ProcessHubCore class.

        Args:
            name (str): The name of the process.
            **kwargs: Additional keyword arguments to pass to the Process constructor.
        """

        Process.__init__(self, name=name, **kwargs)
        # process name to pid map
        self._spawned_processes_map: Dict[str, Optional[int]] = dict()
        self._spawned_cb_processes_map: Dict[str, Dict[str, Optional[ProcessorWorkerDetails]]] = (
            dict()
        )  # separate map for callback worker spawns. unique ID -> dict(unique_name, pid)
        self._thread_shutdown_event = threading.Event()
        self._shutdown_initiated = False

    def signal_handler(self, signum, frame):
        """
        Handles the specified signal by initiating a shutdown and sending a shutdown signal
        to the reporting service.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._thread_shutdown_event.set()
            # raise GenericExitOnSignal

    def kill_process(self, pid: int):
        """
        Terminate a process with the given process ID (pid).

        Args:
            pid (int): The process ID of the process to be terminated.

        Returns:
            None
        """
        p = psutil.Process(pid)
        self._logger.debug(
            'Attempting to send SIGTERM to process ID {} for following command',
            pid,
        )
        p.terminate()
        self._logger.debug(
            'Waiting for 3 seconds to confirm termination of process',
        )
        gone, alive = psutil.wait_procs([p], timeout=3)
        for p_ in alive:
            self._logger.debug(
                'Process ID {} not terminated by SIGTERM. Sending SIGKILL...',
                p_.pid,
            )
            p_.kill()

        for k, v in self._spawned_cb_processes_map.items():
            for unique_worker_entry in v.values():
                if unique_worker_entry is not None and unique_worker_entry.pid == pid:
                    psutil.Process(pid).wait()
                    break

        for k, v in self._spawned_processes_map.items():
            if v is not None and v == pid:
                self._logger.debug('Waiting for process ID {} to join...', pid)
                psutil.Process(pid).wait()
                self._logger.debug('Process ID {} joined...', pid)
                break

    @provide_redis_conn_repsawning_thread
    def internal_state_reporter(self, redis_conn: redis.Redis = None):
        """
        Internal state reporter function that periodically reports the state of spawned processes to Redis
        and pings a reporting service.

        Args:
            redis_conn (redis.Redis, optional): Redis connection object. Defaults to None.
        """
        while not self._thread_shutdown_event.wait(timeout=1):
            proc_id_map = dict()
            for k, v in self._spawned_processes_map.items():
                if v:
                    proc_id_map[k] = v
                else:
                    proc_id_map[k] = -1
            proc_id_map['callback_workers'] = dict()
            for (
                k,
                unique_worker_entries,
            ) in self._spawned_cb_processes_map.items():
                proc_id_map['callback_workers'][k] = dict()
                for (
                    worker_unique_id,
                    worker_process_details,
                ) in unique_worker_entries.items():
                    if worker_process_details is not None:
                        proc_id_map['callback_workers'][k][worker_unique_id] = {
                            'pid': worker_process_details.pid,
                            'id': worker_process_details.unique_name,
                        }
                    else:
                        proc_id_map['callback_workers'][k][worker_unique_id] = {
                            'pid': 'null',
                            'id': '',
                        }
            proc_id_map['callback_workers'] = json.dumps(
                proc_id_map['callback_workers'],
            )
            redis_conn.hset(
                name=f'powerloom:relayer:TxWorkerLauncherCore:{settings.protocol_state_address.lower()}:Processes',
                mapping=proc_id_map,
            )

        self._logger.error(
            (
                'Caught thread shutdown notification event. Deleting process'
                ' worker map in redis...'
            ),
        )
        redis_conn.delete(
            f'powerloom:relayer:TxWorkerLauncherCore:{settings.protocol_state_address.lower()}:Processes',
        )

    def _kill_all_children(self, core_workers=True):
        """
        Terminate all the child processes spawned by the current process.

        Args:
            core_workers (bool): If True, terminate all the core workers as well.
        """
        self._logger.error('Waiting on spawned callback workers to join...')
        for (
            worker_class_name,
            unique_worker_entries,
        ) in self._spawned_cb_processes_map.items():
            procs = []
            for (
                worker_unique_id,
                worker_unique_process_details,
            ) in unique_worker_entries.items():
                if worker_unique_process_details is not None and worker_unique_process_details.pid:
                    self._logger.error(
                        (
                            'Waiting on spawned callback worker {} | Unique'
                            ' ID {} | PID {}  to join...'
                        ),
                        worker_class_name,
                        worker_unique_id,
                        worker_unique_process_details.pid,
                    )
                    _ = psutil.Process(pid=worker_unique_process_details.pid)
                    procs.append(_)
                    _.terminate()
            gone, alive = psutil.wait_procs(procs, timeout=3)
            for p in alive:
                self._logger.error(
                    'Sending SIGKILL to spawned callback worker {} after not exiting on SIGTERM | PID {}',
                    worker_class_name,
                    p.pid,
                )
                p.kill()
        self._spawned_cb_processes_map = dict()
        if core_workers:
            logger.error(
                'Waiting on spawned core workers to join... {}',
                self._spawned_processes_map,
            )
            procs = []
            for (
                worker_class_name,
                worker_pid,
            ) in self._spawned_processes_map.items():
                self._logger.error(
                    'spawned Process Pid to wait on {}',
                    worker_pid,
                )
                if worker_pid is not None:
                    self._logger.error(
                        (
                            'Waiting on spawned core worker {} | PID {}  to'
                            ' join...'
                        ),
                        worker_class_name,
                        worker_pid,
                    )
                    _ = psutil.Process(worker_pid)
                    procs.append(_)
                    _.terminate()
            gone, alive = psutil.wait_procs(procs, timeout=3)
            for p in alive:
                self._logger.error(
                    'Sending SIGKILL to spawned core worker after not exiting on SIGTERM | PID {}',
                    p.pid,
                )
                p.kill()
            self._spawned_processes_map = dict()

    def _launch_signing_cb_workers(self):
        """
        Launches signing workers based on the configuration specified in the settings.
        Each worker signs snapshot submissions on behalf of the snapshotter and is launched as a separate process.
        """
        self._logger.debug('=' * 80)
        self._logger.debug('Launching Snapshot Signing Workers')

        # Starting Snapshot workers
        self._spawned_cb_processes_map['signing_workers'] = dict()

        for idx in range(len(settings.signers)):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = (
                f'Powerloom|Relayer|TxWorker:{settings.protocol_state_address.lower()[:5]}' +
                '-' +
                unique_id
            )
            snapshot_worker_obj: Process = TxWorker(
                name=unique_name, worker_idx=idx,
            )
            snapshot_worker_obj.start()
            self._spawned_cb_processes_map['signing_workers'].update(
                {
                    unique_id: ProcessorWorkerDetails(
                        unique_name=unique_name, pid=snapshot_worker_obj.pid,
                    ),
                },
            )
            self._logger.debug(
                (
                    'Tx Worker Core launched process {} for'
                    ' worker type {} with PID: {}'
                ),
                unique_name,
                'signing_workers',
                snapshot_worker_obj.pid,
            )

    def _launch_checking_cb_workers(self):
        """
        Launches signing workers based on the configuration specified in the settings.
        Each worker signs snapshot submissions on behalf of the snapshotter and is launched as a separate process.
        """
        self._logger.debug('=' * 80)
        self._logger.debug('Launching Snapshot Checking Workers')

        # Starting Snapshot workers
        self._spawned_cb_processes_map['checking_workers'] = dict()

        for _ in range(len(settings.signers)):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = (
                f'TxCheckerWorker:{settings.protocol_state_address.lower()[:5]}' +
                '-' +
                unique_id
            )
            snapshot_worker_obj: Process = TxChecker(
                name=unique_name,
            )
            snapshot_worker_obj.start()
            self._spawned_cb_processes_map['checking_workers'].update(
                {
                    unique_id: ProcessorWorkerDetails(
                        unique_name=unique_name, pid=snapshot_worker_obj.pid,
                    ),
                },
            )
            self._logger.debug(
                (
                    'Tx Worker Core launched process {} for'
                    ' worker type {} with PID: {}'
                ),
                unique_name,
                'signing_workers',
                snapshot_worker_obj.pid,
            )

    def _launch_all_children(self):
        """
        Launches all the child processes for the process hub core.
        """
        self._logger.debug('=' * 80)
        self._launch_signing_cb_workers()
        self._launch_checking_cb_workers()

    def _set_start_time(self):
        self._redis_conn_sync.set(
            tx_launcher_core_start_timestamp(),
            str(int(time.time())),
        )

    @cleanup_proc_hub_children
    def run(self) -> None:
        """
        Runs the Process Hub Core.

        Sets up signal handlers, resource limits, Redis connection pool, Anchor RPC helper,
        Protocol State contract, source chain block time, epoch size, snapshot callback workers,
        internal state reporter, RabbitMQ consumer, and raises a SelfExitException to exit the process.
        """
        self._logger = logger.bind(module='Powerloom|ProcessHub|Core')

        for signame in [SIGINT, SIGTERM, SIGQUIT, SIGCHLD]:
            signal(signame, self.signal_handler)

        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )
        self._redis_connection_pool_sync = redis.BlockingConnectionPool(
            **REDIS_CONN_CONF,
        )
        self._redis_conn_sync = redis.Redis(
            connection_pool=self._redis_connection_pool_sync,
        )

        self._launch_all_children()

        self._logger.debug(
            'Starting Internal Process State reporter for Signer Hub Core...',
        )
        self._logger.debug('Starting Signer Hub Core...')
        self._set_start_time()
        self._reporter_thread = Thread(target=self.internal_state_reporter)
        self._reporter_thread.start()
        self._reporter_thread.join()


if __name__ == '__main__':
    p = TxWorkerLauncherCore(name='Powerloom|Relayer|TxWorkerLauncherCore')
    p.start()
    while p.is_alive():
        logger.debug(
            'Tx worker launcher core is still alive. waiting on it to join...',
        )
        try:
            p.join()
        except:
            pass
