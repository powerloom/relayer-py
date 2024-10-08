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

# Redis connection configuration
REDIS_CONN_CONF = {
    'host': settings.redis.host,
    'port': settings.redis.port,
    'password': settings.redis.password,
    'db': settings.redis.db,
    'retry_on_error': [redis.exceptions.ReadOnlyError],
}


class TxWorkerLauncherCore(Process):
    """
    A core class for launching and managing transaction worker processes.

    This class is responsible for initializing, launching, and managing various
    worker processes related to transaction processing and checking.
    """

    _redis_connection_pool_sync: redis.BlockingConnectionPool
    _redis_conn_sync: redis.Redis

    def __init__(self, name, **kwargs):
        """
        Initialize the TxWorkerLauncherCore.

        Args:
            name (str): The name of the process.
            **kwargs: Additional keyword arguments to pass to the Process constructor.
        """
        Process.__init__(self, name=name, **kwargs)
        # Process name to PID map
        self._spawned_processes_map: Dict[str, Optional[int]] = dict()
        # Separate map for callback worker spawns. unique ID -> dict(unique_name, pid)
        self._spawned_cb_processes_map: Dict[
            str,
            Dict[str, Optional[ProcessorWorkerDetails]],
        ] = dict()
        self._thread_shutdown_event = threading.Event()
        self._shutdown_initiated = False

    def signal_handler(self, signum, frame):
        """
        Handle the specified signal by initiating a shutdown.

        Args:
            signum (int): The signal number.
            frame (frame): The current stack frame.
        """
        if signum in [SIGINT, SIGTERM, SIGQUIT]:
            self._shutdown_initiated = True
            self._thread_shutdown_event.set()

    def kill_process(self, pid: int):
        """
        Terminate a process with the given process ID (pid).

        Args:
            pid (int): The process ID of the process to be terminated.
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

        # Wait for callback workers to join
        for k, v in self._spawned_cb_processes_map.items():
            for unique_worker_entry in v.values():
                if unique_worker_entry is not None and unique_worker_entry.pid == pid:
                    psutil.Process(pid).wait()
                    break

        # Wait for core workers to join
        for k, v in self._spawned_processes_map.items():
            if v is not None and v == pid:
                self._logger.debug('Waiting for process ID {} to join...', pid)
                psutil.Process(pid).wait()
                self._logger.debug('Process ID {} joined...', pid)
                break

    @provide_redis_conn_repsawning_thread
    def internal_state_reporter(self, redis_conn: redis.Redis = None):
        """
        Periodically report the state of spawned processes to Redis.

        This method runs in a separate thread and updates Redis with the current
        state of all spawned processes until a shutdown is initiated.

        Args:
            redis_conn (redis.Redis, optional): Redis connection object. Defaults to None.
        """
        while not self._thread_shutdown_event.wait(timeout=1):
            proc_id_map = self._get_process_id_map()
            self._update_redis_with_process_map(redis_conn, proc_id_map)

        self._logger.error(
            'Caught thread shutdown notification event. Deleting process worker map in redis...',
        )
        self._delete_process_map_from_redis(redis_conn)

    def _get_process_id_map(self):
        """
        Get a mapping of all spawned processes and their IDs.

        Returns:
            dict: A dictionary containing process names and their IDs.
        """
        proc_id_map = {
            k: v if v else -1 for k,
            v in self._spawned_processes_map.items()
        }
        proc_id_map['callback_workers'] = self._get_callback_workers_map()
        return proc_id_map

    def _get_callback_workers_map(self):
        """
        Get a mapping of all callback workers and their details.

        Returns:
            dict: A dictionary containing callback worker details.
        """
        callback_workers = {}
        for k, unique_worker_entries in self._spawned_cb_processes_map.items():
            callback_workers[k] = {
                worker_id: {
                    'pid': details.pid if details else 'null',
                    'id': details.unique_name if details else '',
                }
                for worker_id, details in unique_worker_entries.items()
            }
        return json.dumps(callback_workers)

    def _update_redis_with_process_map(self, redis_conn, proc_id_map):
        """
        Update Redis with the current process map.

        Args:
            redis_conn (redis.Redis): Redis connection object.
            proc_id_map (dict): The process ID map to store in Redis.
        """
        redis_conn.hset(
            name=f'powerloom:relayer:TxWorkerLauncherCore:{settings.protocol_state_address.lower()}:Processes',
            mapping=proc_id_map,
        )

    def _delete_process_map_from_redis(self, redis_conn):
        """
        Delete the process map from Redis.

        Args:
            redis_conn (redis.Redis): Redis connection object.
        """
        redis_conn.delete(
            f'powerloom:relayer:TxWorkerLauncherCore:{settings.protocol_state_address.lower()}:Processes',
        )

    def _kill_all_children(self, core_workers=True):
        """
        Terminate all child processes spawned by the current process.

        Args:
            core_workers (bool): If True, terminate all the core workers as well.
        """
        self._kill_callback_workers()
        if core_workers:
            self._kill_core_workers()

    def _kill_callback_workers(self):
        """Terminate all callback worker processes."""
        self._logger.error('Waiting on spawned callback workers to join...')
        for worker_class_name, unique_worker_entries in self._spawned_cb_processes_map.items():
            self._terminate_worker_group(
                worker_class_name, unique_worker_entries,
            )
        self._spawned_cb_processes_map = dict()

    def _kill_core_workers(self):
        """Terminate all core worker processes."""
        self._logger.error(
            'Waiting on spawned core workers to join... {}', self._spawned_processes_map,
        )
        procs = []
        for worker_class_name, worker_pid in self._spawned_processes_map.items():
            if worker_pid is not None:
                self._logger.error(
                    'Waiting on spawned core worker {} | PID {} to join...', worker_class_name, worker_pid,
                )
                procs.append(psutil.Process(worker_pid))
        self._terminate_processes(procs)
        self._spawned_processes_map = dict()

    def _terminate_worker_group(self, worker_class_name, unique_worker_entries):
        """
        Terminate a group of worker processes.

        Args:
            worker_class_name (str): The class name of the workers.
            unique_worker_entries (dict): A dictionary of worker entries.
        """
        procs = []
        for worker_unique_id, worker_unique_process_details in unique_worker_entries.items():
            if worker_unique_process_details is not None and worker_unique_process_details.pid:
                self._logger.error(
                    'Waiting on spawned callback worker {} | Unique ID {} | PID {} to join...',
                    worker_class_name, worker_unique_id, worker_unique_process_details.pid,
                )
                procs.append(
                    psutil.Process(
                    pid=worker_unique_process_details.pid,
                    ),
                )
        self._terminate_processes(procs, worker_class_name)

    def _terminate_processes(self, procs, worker_class_name=None):
        """
        Terminate a list of processes.

        Args:
            procs (list): A list of Process objects to terminate.
            worker_class_name (str, optional): The class name of the workers. Defaults to None.
        """
        for p in procs:
            p.terminate()
        gone, alive = psutil.wait_procs(procs, timeout=3)
        for p in alive:
            self._logger.error(
                'Sending SIGKILL to spawned {} worker after not exiting on SIGTERM | PID {}',
                worker_class_name or 'core', p.pid,
            )
            p.kill()

    def _launch_signing_cb_workers(self):
        """
        Launch signing callback workers based on the configuration specified in the settings.

        Each worker signs snapshot submissions on behalf of the snapshotter and is launched as a separate process.
        """
        self._logger.debug('=' * 80)
        self._logger.debug('Launching Snapshot Signing Workers')

        self._spawned_cb_processes_map['signing_workers'] = dict()

        for idx in range(len(settings.signers)):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = f'Powerloom|Relayer|TxWorker:{settings.protocol_state_address.lower()[:5]}-{unique_id}'
            snapshot_worker_obj: Process = TxWorker(
                name=unique_name, worker_idx=idx,
            )
            snapshot_worker_obj.start()
            self._spawned_cb_processes_map['signing_workers'][unique_id] = ProcessorWorkerDetails(
                unique_name=unique_name, pid=snapshot_worker_obj.pid,
            )
            self._logger.debug(
                'Tx Worker Core launched process {} for worker type {} with PID: {}',
                unique_name, 'signing_workers', snapshot_worker_obj.pid,
            )

    def _launch_checking_cb_workers(self):
        """
        Launch checking callback workers based on the configuration specified in the settings.

        Each worker checks snapshot submissions and is launched as a separate process.
        """
        self._logger.debug('=' * 80)
        self._logger.debug('Launching Snapshot Checking Workers')

        self._spawned_cb_processes_map['checking_workers'] = dict()

        for _ in range(len(settings.signers)):
            unique_id = str(uuid.uuid4())[:5]
            unique_name = f'TxCheckerWorker:{settings.protocol_state_address.lower()[:5]}-{unique_id}'
            snapshot_worker_obj: Process = TxChecker(name=unique_name)
            snapshot_worker_obj.start()
            self._spawned_cb_processes_map['checking_workers'][unique_id] = ProcessorWorkerDetails(
                unique_name=unique_name, pid=snapshot_worker_obj.pid,
            )
            self._logger.debug(
                'Tx Worker Core launched process {} for worker type {} with PID: {}',
                unique_name, 'checking_workers', snapshot_worker_obj.pid,
            )

    def _launch_all_children(self):
        """Launch all child processes for the process hub core."""
        self._logger.debug('=' * 80)
        self._launch_signing_cb_workers()
        self._launch_checking_cb_workers()

    def _set_start_time(self):
        """Set the start time of the TxWorkerLauncherCore in Redis."""
        self._redis_conn_sync.set(
            tx_launcher_core_start_timestamp(),
            str(int(time.time())),
        )

    @cleanup_proc_hub_children
    def run(self) -> None:
        """
        Run the TxWorkerLauncherCore process.

        This method sets up signal handlers, resource limits, Redis connection pool,
        launches child processes, starts the internal state reporter, and manages
        the lifecycle of the TxWorkerLauncherCore.
        """
        self._logger = logger.bind(module='Powerloom|ProcessHub|Core')

        # Set up signal handlers
        for signame in [SIGINT, SIGTERM, SIGQUIT, SIGCHLD]:
            signal(signame, self.signal_handler)

        # Set resource limits
        soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        resource.setrlimit(
            resource.RLIMIT_NOFILE,
            (settings.rlimit.file_descriptors, hard),
        )

        # Set up Redis connection
        self._redis_connection_pool_sync = redis.BlockingConnectionPool(
            **REDIS_CONN_CONF,
        )
        self._redis_conn_sync = redis.Redis(
            connection_pool=self._redis_connection_pool_sync,
        )

        # Launch child processes
        self._launch_all_children()

        # Start internal state reporter
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
