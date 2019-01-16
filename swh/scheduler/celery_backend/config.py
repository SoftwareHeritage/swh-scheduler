# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import itertools
import importlib
import logging
import os
import urllib.parse

from celery import Celery
from celery.signals import setup_logging, celeryd_after_setup
from celery.utils.log import ColorFormatter
from celery.worker.control import Panel

from kombu import Exchange, Queue
from kombu.five import monotonic as _monotonic

import requests

from swh.scheduler.task import Task

from swh.core.config import load_named_config
from swh.core.logger import JournalHandler

DEFAULT_CONFIG_NAME = 'worker'
CONFIG_NAME_ENVVAR = 'SWH_WORKER_INSTANCE'
CONFIG_NAME_TEMPLATE = 'worker/%s'

DEFAULT_CONFIG = {
    'task_broker': ('str', 'amqp://guest@localhost//'),
    'result_backend': ('str', 'rpc://'),
    'task_modules': ('list[str]', []),
    'task_queues': ('list[str]', []),
    'task_soft_time_limit': ('int', 0),
}


@setup_logging.connect
def setup_log_handler(loglevel=None, logfile=None, format=None,
                      colorize=None, **kwargs):
    """Setup logging according to Software Heritage preferences.

    We use the command-line loglevel for tasks only, as we never
    really care about the debug messages from celery.
    """
    if loglevel is None:
        loglevel = logging.DEBUG
    if isinstance(loglevel, str):
        loglevel = logging._nameToLevel[loglevel]

    formatter = logging.Formatter(format)

    root_logger = logging.getLogger('')
    root_logger.setLevel(logging.INFO)

    if loglevel == logging.DEBUG:
        color_formatter = ColorFormatter(format) if colorize else formatter
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(color_formatter)
        root_logger.addHandler(console)

    systemd_journal = JournalHandler()
    systemd_journal.setLevel(logging.DEBUG)
    systemd_journal.setFormatter(formatter)
    root_logger.addHandler(systemd_journal)

    logging.getLogger('celery').setLevel(logging.INFO)
    # Silence amqp heartbeat_tick messages
    logger = logging.getLogger('amqp')
    logger.addFilter(lambda record: not record.msg.startswith(
        'heartbeat_tick'))
    logger.setLevel(logging.DEBUG)
    # Silence useless "Starting new HTTP connection" messages
    logging.getLogger('urllib3').setLevel(logging.WARNING)

    logging.getLogger('swh').setLevel(loglevel)
    # get_task_logger makes the swh tasks loggers children of celery.task
    logging.getLogger('celery.task').setLevel(loglevel)
    return loglevel


@celeryd_after_setup.connect
def setup_queues_and_tasks(sender, instance, **kwargs):
    """Signal called on worker start.

    This automatically registers swh.scheduler.task.Task subclasses as
    available celery tasks.

    This also subscribes the worker to the "implicit" per-task queues defined
    for these task classes.

    """

    for module_name in itertools.chain(
            # celery worker -I flag
            instance.app.conf['include'],
            # set from the celery / swh worker instance configuration file
            instance.app.conf['imports'],
    ):
        module = importlib.import_module(module_name)
        for name in dir(module):
            obj = getattr(module, name)
            if (
                    isinstance(obj, type)
                    and issubclass(obj, Task)
                    and obj != Task  # Don't register the abstract class itself
            ):
                class_name = '%s.%s' % (module_name, name)
                register_task_class(instance.app, class_name, obj)

    for task_name in instance.app.tasks:
        if task_name.startswith('swh.'):
            instance.app.amqp.queues.select_add(task_name)


@Panel.register
def monotonic(state):
    """Get the current value for the monotonic clock"""
    return {'monotonic': _monotonic()}


def route_for_task(name, args, kwargs, options, task=None, **kw):
    """Route tasks according to the task_queue attribute in the task class"""
    if name is not None and name.startswith('swh.'):
        return {'queue': name}


def get_queue_stats(app, queue_name):
    """Get the statistics regarding a queue on the broker.

    Arguments:
      queue_name: name of the queue to check

    Returns a dictionary raw from the RabbitMQ management API;
    or `None` if the current configuration does not use RabbitMQ.

    Interesting keys:
     - consumers (number of consumers for the queue)
     - messages (number of messages in queue)
     - messages_unacknowledged (number of messages currently being
       processed)

    Documentation: https://www.rabbitmq.com/management.html#http-api
    """

    conn_info = app.connection().info()
    if conn_info['transport'] == 'memory':
        # We're running in a test environment, without RabbitMQ.
        return None
    url = 'http://{hostname}:{port}/api/queues/{vhost}/{queue}'.format(
        hostname=conn_info['hostname'],
        port=conn_info['port'] + 10000,
        vhost=urllib.parse.quote(conn_info['virtual_host'], safe=''),
        queue=urllib.parse.quote(queue_name, safe=''),
    )
    credentials = (conn_info['userid'], conn_info['password'])
    r = requests.get(url, auth=credentials)
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        raise ValueError('Got error %s when reading queue stats: %s' % (
            r.status_code, r.json()))
    return r.json()


def get_queue_length(app, queue_name):
    """Shortcut to get a queue's length"""
    stats = get_queue_stats(app, queue_name)
    if stats:
        return stats.get('messages')


def register_task_class(app, name, cls):
    """Register a class-based task under the given name"""
    if name in app.tasks:
        return

    task_instance = cls()
    task_instance.name = name
    app.register_task(task_instance)


INSTANCE_NAME = os.environ.get(CONFIG_NAME_ENVVAR)
if INSTANCE_NAME:
    CONFIG_NAME = CONFIG_NAME_TEMPLATE % INSTANCE_NAME
else:
    CONFIG_NAME = DEFAULT_CONFIG_NAME

# Load the Celery config
CONFIG = load_named_config(CONFIG_NAME, DEFAULT_CONFIG)

# Celery Queues
CELERY_QUEUES = [Queue('celery', Exchange('celery'), routing_key='celery')]

for queue in CONFIG['task_queues']:
    CELERY_QUEUES.append(Queue(queue, Exchange(queue), routing_key=queue))

CELERY_DEFAULT_CONFIG = dict(
    # Timezone configuration: all in UTC
    enable_utc=True,
    timezone='UTC',
    # Imported modules
    imports=CONFIG['task_modules'],
    # Time (in seconds, or a timedelta object) for when after stored task
    # tombstones will be deleted. None means to never expire results.
    result_expires=None,
    # A string identifying the default serialization method to use. Can
    # be json (default), pickle, yaml, msgpack, or any custom
    # serialization methods that have been registered with
    task_serializer='msgpack',
    # Result serialization format
    result_serializer='msgpack',
    # Late ack means the task messages will be acknowledged after the task has
    # been executed, not just before, which is the default behavior.
    task_acks_late=True,
    # A string identifying the default serialization method to use.
    # Can be pickle (default), json, yaml, msgpack or any custom serialization
    # methods that have been registered with kombu.serialization.registry
    accept_content=['msgpack', 'json'],
    # If True the task will report its status as “started”
    # when the task is executed by a worker.
    task_track_started=True,
    # Default compression used for task messages. Can be gzip, bzip2
    # (if available), or any custom compression schemes registered
    # in the Kombu compression registry.
    # result_compression='bzip2',
    # task_compression='bzip2',
    # Disable all rate limits, even if tasks has explicit rate limits set.
    # (Disabling rate limits altogether is recommended if you don’t have any
    # tasks using them.)
    worker_disable_rate_limits=True,
    # Task hard time limit in seconds. The worker processing the task will be
    # killed and replaced with a new one when this is exceeded.
    # task_time_limit=3600,
    # Task soft time limit in seconds.
    # The SoftTimeLimitExceeded exception will be raised when this is exceeded.
    # The task can catch this to e.g. clean up before the hard time limit
    # comes.
    task_soft_time_limit=CONFIG['task_soft_time_limit'],
    # Task routing
    task_routes=route_for_task,
    # Task queues this worker will consume from
    task_queues=CELERY_QUEUES,
    # Allow pool restarts from remote
    worker_pool_restarts=True,
    # Do not prefetch tasks
    worker_prefetch_multiplier=1,
    # Send events
    worker_send_task_events=True,
    # Do not send useless task_sent events
    task_send_sent_event=False,
    )

# Instantiate the Celery app
app = Celery(broker=CONFIG['task_broker'],
             backend=CONFIG['result_backend'],
             task_cls='swh.scheduler.task:SWHTask')
app.add_defaults(CELERY_DEFAULT_CONFIG)

# XXX for BW compat
Celery.get_queue_length = get_queue_length
