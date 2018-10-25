# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import logging
import os
import urllib.parse

from celery import Celery
from celery.signals import setup_logging
from celery.utils.log import ColorFormatter
from celery.worker.control import Panel

from kombu import Exchange, Queue
from kombu.five import monotonic as _monotonic

import requests

from swh.core.config import load_named_config
from swh.core.logger import JournalHandler

DEFAULT_CONFIG_NAME = 'worker'
CONFIG_NAME_ENVVAR = 'SWH_WORKER_INSTANCE'
CONFIG_NAME_TEMPLATE = 'worker/%s'

DEFAULT_CONFIG = {
    'task_broker': ('str', 'amqp://guest@localhost//'),
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

    celery_logger = logging.getLogger('celery')
    celery_logger.setLevel(logging.INFO)

    # Silence useless "Starting new HTTP connection" messages
    urllib3_logger = logging.getLogger('urllib3')
    urllib3_logger.setLevel(logging.WARNING)

    swh_logger = logging.getLogger('swh')
    swh_logger.setLevel(loglevel)

    # get_task_logger makes the swh tasks loggers children of celery.task
    celery_task_logger = logging.getLogger('celery.task')
    celery_task_logger.setLevel(loglevel)


@Panel.register
def monotonic(state):
    """Get the current value for the monotonic clock"""
    return {'monotonic': _monotonic()}


class TaskRouter:
    """Route tasks according to the task_queue attribute in the task class"""
    def route_for_task(self, task, args=None, kwargs=None):
        task_class = app.tasks[task]
        if hasattr(task_class, 'task_queue'):
            return {'queue': task_class.task_queue}
        return None


class CustomCelery(Celery):
    def get_queue_stats(self, queue_name):
        """Get the statistics regarding a queue on the broker.

        Arguments:
          queue_name: name of the queue to check

        Returns a dictionary raw from the RabbitMQ management API.

        Interesting keys:
         - consumers (number of consumers for the queue)
         - messages (number of messages in queue)
         - messages_unacknowledged (number of messages currently being
           processed)

        Documentation: https://www.rabbitmq.com/management.html#http-api
        """

        conn_info = self.connection().info()
        url = 'http://{hostname}:{port}/api/queues/{vhost}/{queue}'.format(
            hostname=conn_info['hostname'],
            port=conn_info['port'] + 10000,
            vhost=urllib.parse.quote(conn_info['virtual_host'], safe=''),
            queue=urllib.parse.quote(queue_name, safe=''),
        )
        credentials = (conn_info['userid'], conn_info['password'])
        r = requests.get(url, auth=credentials)
        if r.status_code != 200:
            raise ValueError('Got error %s when reading queue stats: %s' % (
                r.status_code, r.json()))
        return r.json()

    def get_queue_length(self, queue_name):
        """Shortcut to get a queue's length"""
        return self.get_queue_stats(queue_name)['messages']


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

# Instantiate the Celery app
app = CustomCelery()
app.conf.update(
    # The broker
    BROKER_URL=CONFIG['task_broker'],
    # Timezone configuration: all in UTC
    CELERY_ENABLE_UTC=True,
    CELERY_TIMEZONE='UTC',
    # Imported modules
    CELERY_IMPORTS=CONFIG['task_modules'],
    # Time (in seconds, or a timedelta object) for when after stored task
    # tombstones will be deleted. None means to never expire results.
    CELERY_TASK_RESULT_EXPIRES=None,
    # A string identifying the default serialization method to use. Can
    # be json (default), pickle, yaml, msgpack, or any custom
    # serialization methods that have been registered with
    CELERY_TASK_SERIALIZER='msgpack',
    # Result serialization format
    CELERY_RESULT_SERIALIZER='msgpack',
    # Late ack means the task messages will be acknowledged after the task has
    # been executed, not just before, which is the default behavior.
    CELERY_ACKS_LATE=True,
    # A string identifying the default serialization method to use.
    # Can be pickle (default), json, yaml, msgpack or any custom serialization
    # methods that have been registered with kombu.serialization.registry
    CELERY_ACCEPT_CONTENT=['msgpack', 'json', 'pickle'],
    # If True the task will report its status as “started”
    # when the task is executed by a worker.
    CELERY_TRACK_STARTED=True,
    # Default compression used for task messages. Can be gzip, bzip2
    # (if available), or any custom compression schemes registered
    # in the Kombu compression registry.
    # CELERY_MESSAGE_COMPRESSION='bzip2',
    # Disable all rate limits, even if tasks has explicit rate limits set.
    # (Disabling rate limits altogether is recommended if you don’t have any
    # tasks using them.)
    CELERY_DISABLE_RATE_LIMITS=True,
    # Task hard time limit in seconds. The worker processing the task will be
    # killed and replaced with a new one when this is exceeded.
    # CELERYD_TASK_TIME_LIMIT=3600,
    # Task soft time limit in seconds.
    # The SoftTimeLimitExceeded exception will be raised when this is exceeded.
    # The task can catch this to e.g. clean up before the hard time limit
    # comes.
    CELERYD_TASK_SOFT_TIME_LIMIT=CONFIG['task_soft_time_limit'],
    # Task routing
    CELERY_ROUTES=TaskRouter(),
    # Task queues this worker will consume from
    CELERY_QUEUES=CELERY_QUEUES,
    # Allow pool restarts from remote
    CELERYD_POOL_RESTARTS=True,
    # Do not prefetch tasks
    CELERYD_PREFETCH_MULTIPLIER=1,
    # Send events
    CELERY_SEND_EVENTS=True,
    # Do not send useless task_sent events
    CELERY_SEND_TASK_SENT_EVENT=False,
)
