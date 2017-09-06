# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import socket

from arrow import utcnow
from kombu import Queue
from celery.events import EventReceiver

from .config import app as main_app
from ..backend import SchedulerBackend


class ReliableEventReceiver(EventReceiver):
    def __init__(self, channel, handlers=None, routing_key='#',
                 node_id=None, app=None, queue_prefix='celeryev',
                 accept=None):
        super(ReliableEventReceiver, self).__init__(
            channel, handlers, routing_key, node_id, app, queue_prefix, accept)

        self.queue = Queue('.'.join([self.queue_prefix, self.node_id]),
                           exchange=self.exchange,
                           routing_key=self.routing_key,
                           auto_delete=False,
                           durable=True,
                           queue_arguments=self._get_queue_arguments())

    def get_consumers(self, consumer, channel):
        return [consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=False,
                         accept=self.accept)]

    def _receive(self, body, message):
        type, body = self.event_from_message(body)
        self.process(type, body, message)

    def process(self, type, event, message):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        handler and handler(event, message)


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
        result = event['result']

        try:
            status = result.get('status')
            if status == 'success':
                status = 'eventful' if result.get('eventful') else 'uneventful'
        except Exception:
            status = 'eventful' if result else 'uneventful'

        queue_action({
            'action': 'end_task_run',
            'args': [event['uuid']],
            'kwargs': {
                'timestamp': utcnow(),
                'status': status,
                'result': result,
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

    recv = ReliableEventReceiver(
        main_app.connection(),
        app=main_app,
        handlers={
            'task-started': task_started,
            'task-result': task_succeeded,
            'task-failed': task_failed,
            '*': catchall_event,
        },
        node_id='listener-%s' % socket.gethostname(),
    )

    recv.capture(limit=None, timeout=None, wakeup=True)


if __name__ == '__main__':
    main_backend = SchedulerBackend()
    event_monitor(main_app, main_backend)