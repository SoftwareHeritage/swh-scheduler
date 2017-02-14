# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import socket

from arrow import utcnow
from kombu import Queue
from kombu.mixins import ConsumerMixin
from celery.events import get_exchange

from .config import app as main_app
from ..backend import SchedulerBackend


# This is a simplified version of celery.events.Receiver, with a persistent
# queue and acked messages, with most of the options stripped down
#
# The original celery.events.Receiver code is available under the following
# license:
#
# Copyright (c) 2015-2016 Ask Solem & contributors.  All rights reserved.
# Copyright (c) 2012-2014 GoPivotal, Inc.  All rights reserved.
# Copyright (c) 2009, 2010, 2011, 2012 Ask Solem, and individual contributors.
#    All rights reserved.
#
# Celery is licensed under The BSD License (3 Clause, also known as
# the new BSD license), whose full-text is available in the top-level
# LICENSE.Celery file.

class ReliableEventsReceiver(ConsumerMixin):
    def __init__(self, app, handlers, queue_id):
        self.app = app
        self.connection = self.app.connection().connection.client
        self.handlers = handlers
        self.queue_id = queue_id
        self.exchange = get_exchange(self.connection)
        self.queue = Queue(queue_id, exchange=self.exchange, routing_key='#',
                           auto_delete=False, durable=True)
        self.accept = set([self.app.conf.CELERY_EVENT_SERIALIZER, 'json'])

    def process(self, type, event, message):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event, message)

    def get_consumers(self, consumer, channel):
        return [consumer(queues=[self.queue],
                         callbacks=[self.receive], no_ack=False,
                         accept=self.accept)]

    def on_consume_ready(self, connection, channel, consumers, **kwargs):
        # When starting to consume, wakeup the workers
        self.app.control.broadcast('heartbeat',
                                   connection=self.connection,
                                   channel=channel)

    def capture(self, limit=None, timeout=None, wakeup=True):
        """Open up a consumer capturing events.

        This has to run in the main process, and it will never
        stop unless forced via :exc:`KeyboardInterrupt` or :exc:`SystemExit`.

        """
        for _ in self.consume(limit=limit, timeout=timeout, wakeup=wakeup):
            pass

    def receive(self, body, message):
        body['local_received'] = utcnow()
        self.process(body['type'], body, message=message)


ACTION_SEND_DELAY = datetime.timedelta(seconds=1.0)
ACTION_QUEUE_MAX_LENGTH = 1000


def event_monitor(app, backend):
    actions = {
        'last_send': utcnow() - 2*ACTION_SEND_DELAY,
        'queue': [],
    }

    def try_perform_actions(actions=actions):
        if not actions['queue']:
            return
        if utcnow() - actions['last_send'] > ACTION_SEND_DELAY or \
           len(actions['queue']) > ACTION_QUEUE_MAX_LENGTH:
            perform_actions(actions)

    def perform_actions(actions, backend=backend):
        action_map = {
            'start_task_run': backend.start_task_run,
            'end_task_run': backend.end_task_run,
        }

        messages = []
        cursor = backend.cursor()
        for action in actions['queue']:
            messages.append(action['message'])
            function = action_map[action['action']]
            args = action.get('args', ())
            kwargs = action.get('kwargs', {})
            kwargs['cursor'] = cursor
            function(*args, **kwargs)

        backend.commit()
        for message in messages:
            message.ack()
        actions['queue'] = []
        actions['last_send'] = utcnow()

    def queue_action(action, actions=actions):
        actions['queue'].append(action)
        try_perform_actions()

    def catchall_event(event, message):
        message.ack()
        try_perform_actions()

    def task_started(event, message):
        queue_action({
            'action': 'start_task_run',
            'args': [event['uuid']],
            'kwargs': {
                'timestamp': utcnow(),
                'metadata': {
                    'worker': event['hostname'],
                },
            },
            'message': message,
        })

    def task_succeeded(event, message):
        status = 'uneventful'
        if 'True' in event['result']:
            status = 'eventful'

        queue_action({
            'action': 'end_task_run',
            'args': [event['uuid']],
            'kwargs': {
                'timestamp': utcnow(),
                'status': status,
            },
            'message': message,
        })

    def task_failed(event, message):
        queue_action({
            'action': 'end_task_run',
            'args': [event['uuid']],
            'kwargs': {
                'timestamp': utcnow(),
                'status': 'failed',
            },
            'message': message,
        })

    recv = ReliableEventsReceiver(
        main_app,
        handlers={
            'task-started': task_started,
            'task-succeeded': task_succeeded,
            'task-failed': task_failed,
            '*': catchall_event,
        },
        queue_id='celeryev.listener-%s' % socket.gethostname(),
    )

    recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == '__main__':
    main_backend = SchedulerBackend()
    event_monitor(main_app, main_backend)
