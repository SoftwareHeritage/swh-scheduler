# Copyright (C) 2020  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json
import logging
import sys

import pika

from swh.core.statsd import statsd
from swh.scheduler import get_scheduler
from swh.scheduler.utils import utcnow

logger = logging.getLogger(__name__)


def get_listener(broker_url, queue_name, scheduler_backend):
    connection = pika.BlockingConnection(pika.URLParameters(broker_url))
    channel = connection.channel()

    channel.queue_declare(queue=queue_name, durable=True)

    exchange = "celeryev"
    routing_key = "#"
    channel.queue_bind(queue=queue_name, exchange=exchange, routing_key=routing_key)

    channel.basic_qos(prefetch_count=1000)

    channel.basic_consume(
        queue=queue_name, on_message_callback=get_on_message(scheduler_backend),
    )

    return channel


def get_on_message(scheduler_backend):
    def on_message(channel, method_frame, properties, body):
        try:
            events = json.loads(body)
        except Exception:
            logger.warning("Could not parse body %r", body)
            events = []

        if not isinstance(events, list):
            events = [events]

        for event in events:
            logger.debug("Received event %r", event)
            process_event(event, scheduler_backend)

        channel.basic_ack(delivery_tag=method_frame.delivery_tag)

    return on_message


def process_event(event, scheduler_backend):
    uuid = event.get("uuid")
    if not uuid:
        return

    event_type = event["type"]
    statsd.increment(
        "swh_scheduler_listener_handled_event_total", tags={"event_type": event_type}
    )

    if event_type == "task-started":
        scheduler_backend.start_task_run(
            uuid, timestamp=utcnow(), metadata={"worker": event.get("hostname")},
        )
    elif event_type == "task-result":
        result = event["result"]

        status = None

        if isinstance(result, dict) and "status" in result:
            status = result["status"]
            if status == "success":
                status = "eventful" if result.get("eventful") else "uneventful"

        if status is None:
            status = "eventful" if result else "uneventful"

        scheduler_backend.end_task_run(
            uuid, timestamp=utcnow(), status=status, result=result
        )
    elif event_type == "task-failed":
        scheduler_backend.end_task_run(uuid, timestamp=utcnow(), status="failed")


if __name__ == "__main__":
    url = sys.argv[1]
    logging.basicConfig(level=logging.DEBUG)
    scheduler_backend = get_scheduler("local", args={"db": "service=swh-scheduler"})
    channel = get_listener(url, "celeryev.test", scheduler_backend)
    logger.info("Start consuming")
    channel.start_consuming()
