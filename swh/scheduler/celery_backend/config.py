# Copyright (C) 2015-2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import functools
import logging
import os
from time import monotonic as _monotonic
import traceback
from typing import Any, Dict
import urllib.parse

from celery import Celery
from celery.signals import celeryd_after_setup, setup_logging, worker_init
from celery.utils.log import ColorFormatter
from celery.worker.control import Panel
from kombu import Exchange, Queue
import pkg_resources
import requests

from swh.core.config import load_named_config, merge_configs
from swh.core.sentry import init_sentry
from swh.scheduler import CONFIG as SWH_CONFIG

try:
    from swh.core.logger import JournalHandler
except ImportError:
    JournalHandler = None  # type: ignore


DEFAULT_CONFIG_NAME = "worker"
CONFIG_NAME_ENVVAR = "SWH_WORKER_INSTANCE"
CONFIG_NAME_TEMPLATE = "worker/%s"

DEFAULT_CONFIG = {
    "task_broker": ("str", "amqp://guest@localhost//"),
    "task_modules": ("list[str]", []),
    "task_queues": ("list[str]", []),
    "task_soft_time_limit": ("int", 0),
}

logger = logging.getLogger(__name__)


# Celery eats tracebacks in signal callbacks, this decorator catches
# and prints them.
# Also tries to notify Sentry if possible.
def _print_errors(f):
    @functools.wraps(f)
    def newf(*args, **kwargs):
        try:
            return f(*args, **kwargs)
        except Exception as exc:
            traceback.print_exc()
            try:
                import sentry_sdk

                sentry_sdk.capture_exception(exc)
            except Exception:
                traceback.print_exc()

    return newf


@setup_logging.connect
@_print_errors
def setup_log_handler(
    loglevel=None,
    logfile=None,
    format=None,
    colorize=None,
    log_console=None,
    log_journal=None,
    **kwargs,
):
    """Setup logging according to Software Heritage preferences.

    We use the command-line loglevel for tasks only, as we never
    really care about the debug messages from celery.
    """
    if loglevel is None:
        loglevel = logging.DEBUG
    if isinstance(loglevel, str):
        loglevel = logging._nameToLevel[loglevel]

    formatter = logging.Formatter(format)

    root_logger = logging.getLogger("")
    root_logger.setLevel(logging.INFO)

    log_target = os.environ.get("SWH_LOG_TARGET", "console")
    if log_target == "console":
        log_console = True
    elif log_target == "journal":
        log_journal = True

    # this looks for log levels *higher* than DEBUG
    if loglevel <= logging.DEBUG and log_console is None:
        log_console = True

    if log_console:
        color_formatter = ColorFormatter(format) if colorize else formatter
        console = logging.StreamHandler()
        console.setLevel(logging.DEBUG)
        console.setFormatter(color_formatter)
        root_logger.addHandler(console)

    if log_journal:
        if not JournalHandler:
            root_logger.warning(
                "JournalHandler is not available, skipping. "
                "Please install swh-core[logging]."
            )
        else:
            systemd_journal = JournalHandler()
            systemd_journal.setLevel(logging.DEBUG)
            systemd_journal.setFormatter(formatter)
            root_logger.addHandler(systemd_journal)

    logging.getLogger("celery").setLevel(logging.INFO)

    # Silence amqp heartbeat_tick messages
    logger = logging.getLogger("amqp")
    logger.addFilter(lambda record: not record.msg.startswith("heartbeat_tick"))
    logger.setLevel(loglevel)

    # Silence useless "Starting new HTTP connection" messages
    logging.getLogger("urllib3").setLevel(logging.WARNING)

    # Completely disable azure logspam
    azure_logger = logging.getLogger("azure.core.pipeline.policies.http_logging_policy")
    azure_logger.setLevel(logging.WARNING)

    logging.getLogger("swh").setLevel(loglevel)
    # get_task_logger makes the swh tasks loggers children of celery.task
    logging.getLogger("celery.task").setLevel(loglevel)
    return loglevel


@celeryd_after_setup.connect
@_print_errors
def setup_queues_and_tasks(sender, instance, **kwargs):
    """Signal called on worker start.

    This automatically registers swh.scheduler.task.Task subclasses as
    available celery tasks.

    This also subscribes the worker to the "implicit" per-task queues defined
    for these task classes.

    """
    logger.info("Setup Queues & Tasks for %s", sender)
    instance.app.conf["worker_name"] = sender


@worker_init.connect
@_print_errors
def on_worker_init(*args, **kwargs):
    try:
        from sentry_sdk.integrations.celery import CeleryIntegration
    except ImportError:
        integrations = []
    else:
        integrations = [CeleryIntegration()]
    sentry_dsn = None  # will be set in `init_sentry` function
    init_sentry(sentry_dsn, integrations=integrations)


@Panel.register
def monotonic(state):
    """Get the current value for the monotonic clock"""
    return {"monotonic": _monotonic()}


def route_for_task(name, args, kwargs, options, task=None, **kw):
    """Route tasks according to the task_queue attribute in the task class"""
    if name is not None and name.startswith("swh."):
        return {"queue": name}


def get_queue_stats(app, queue_name):
    """Get the statistics regarding a queue on the broker.

    Arguments:
      queue_name: name of the queue to check

    Returns a dictionary raw from the RabbitMQ management API;
    or `None` if the current configuration does not use RabbitMQ.

    Interesting keys:
     - Consumers (number of consumers for the queue)
     - messages (number of messages in queue)
     - messages_unacknowledged (number of messages currently being
       processed)

    Documentation: https://www.rabbitmq.com/management.html#http-api
    """

    conn_info = app.connection().info()
    if conn_info["transport"] == "memory":
        # We're running in a test environment, without RabbitMQ.
        return None
    url = "http://{hostname}:{port}/api/queues/{vhost}/{queue}".format(
        hostname=conn_info["hostname"],
        port=conn_info["port"] + 10000,
        vhost=urllib.parse.quote(conn_info["virtual_host"], safe=""),
        queue=urllib.parse.quote(queue_name, safe=""),
    )
    credentials = (conn_info["userid"], conn_info["password"])
    r = requests.get(url, auth=credentials)
    if r.status_code == 404:
        return {}
    if r.status_code != 200:
        raise ValueError(
            "Got error %s when reading queue stats: %s" % (r.status_code, r.json())
        )
    return r.json()


def get_queue_length(app, queue_name):
    """Shortcut to get a queue's length"""
    stats = get_queue_stats(app, queue_name)
    if stats:
        return stats.get("messages")


def register_task_class(app, name, cls):
    """Register a class-based task under the given name"""
    if name in app.tasks:
        return

    task_instance = cls()
    task_instance.name = name
    app.register_task(task_instance)


INSTANCE_NAME = os.environ.get(CONFIG_NAME_ENVVAR)
CONFIG_NAME = os.environ.get("SWH_CONFIG_FILENAME")
CONFIG = {}  # type: Dict[str, Any]

if CONFIG_NAME:
    # load the celery config from the main config file given as
    # SWH_CONFIG_FILENAME environment variable.
    # This is expected to have a [celery] section in which we have the
    # celery specific configuration.
    SWH_CONFIG.clear()
    SWH_CONFIG.update(load_named_config(CONFIG_NAME))
    CONFIG = SWH_CONFIG.get("celery", {})

if not CONFIG:
    # otherwise, back to compat config loading mechanism
    if INSTANCE_NAME:
        CONFIG_NAME = CONFIG_NAME_TEMPLATE % INSTANCE_NAME
    else:
        CONFIG_NAME = DEFAULT_CONFIG_NAME

    # Load the Celery config
    CONFIG = load_named_config(CONFIG_NAME, DEFAULT_CONFIG)

CONFIG.setdefault("task_modules", [])
# load tasks modules declared as plugin entry points
for entrypoint in pkg_resources.iter_entry_points("swh.workers"):
    worker_registrer_fn = entrypoint.load()
    # The registry function is expected to return a dict which the 'tasks' key
    # is a string (or a list of strings) with the name of the python module in
    # which celery tasks are defined.
    task_modules = worker_registrer_fn().get("task_modules", [])
    CONFIG["task_modules"].extend(task_modules)

# Celery Queues
CELERY_QUEUES = [Queue("celery", Exchange("celery"), routing_key="celery")]

CELERY_DEFAULT_CONFIG = dict(
    # Timezone configuration: all in UTC
    enable_utc=True,
    timezone="UTC",
    # Imported modules
    imports=CONFIG.get("task_modules", []),
    # Time (in seconds, or a timedelta object) for when after stored task
    # tombstones will be deleted. None means to never expire results.
    result_expires=None,
    # A string identifying the default serialization method to use. Can
    # be json (default), pickle, yaml, msgpack, or any custom
    # serialization methods that have been registered with
    task_serializer="msgpack",
    # Result serialization format
    result_serializer="msgpack",
    # Acknowledge tasks as soon as they're received. We can do this as we have
    # external monitoring to decide if we need to retry tasks.
    task_acks_late=False,
    # A string identifying the default serialization method to use.
    # Can be pickle (default), json, yaml, msgpack or any custom serialization
    # methods that have been registered with kombu.serialization.registry
    accept_content=["msgpack", "json"],
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
    # Task routing
    task_routes=route_for_task,
    # Allow pool restarts from remote
    worker_pool_restarts=True,
    # Do not prefetch tasks
    worker_prefetch_multiplier=1,
    # Send events
    worker_send_task_events=True,
    # Do not send useless task_sent events
    task_send_sent_event=False,
)


def build_app(config=None):
    config = merge_configs(
        {k: v for (k, (_, v)) in DEFAULT_CONFIG.items()}, config or {}
    )

    config["task_queues"] = CELERY_QUEUES + [
        Queue(queue, Exchange(queue), routing_key=queue)
        for queue in config.get("task_queues", ())
    ]
    logger.debug("Creating a Celery app with %s", config)

    # Instantiate the Celery app
    app = Celery(broker=config["task_broker"], task_cls="swh.scheduler.task:SWHTask")
    app.add_defaults(CELERY_DEFAULT_CONFIG)
    app.add_defaults(config)
    return app


app = build_app(CONFIG)

# XXX for BW compat
Celery.get_queue_length = get_queue_length
