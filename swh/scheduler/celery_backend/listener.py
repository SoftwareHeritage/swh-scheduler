# Copyright (C) 2015-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import logging
import time
import socket
import sys

import click

from arrow import utcnow
from kombu import Queue

import celery
from celery.events import EventReceiver


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
                           durable=True)

    def get_consumers(self, consumer, channel):
        return [consumer(queues=[self.queue],
                         callbacks=[self._receive], no_ack=False,
                         accept=self.accept)]

    def _receive(self, bodies, message):
        logging.debug('## event-receiver: bodies: %s' % bodies)
        logging.debug('## event-receiver: message: %s' % message)
        if not isinstance(bodies, list):  # celery<4 returned body as element
            bodies = [bodies]
        for body in bodies:
            type, body = self.event_from_message(body)
            self.process(type, body, message)

    def process(self, type, event, message):
        """Process the received event by dispatching it to the appropriate
        handler."""
        handler = self.handlers.get(type) or self.handlers.get('*')
        logging.debug('## event-receiver: type: %s' % type)
        logging.debug('## event-receiver: event: %s' % event)
        logging.debug('## event-receiver: handler: %s' % handler)
        handler and handler(event, message)


ACTION_SEND_DELAY = datetime.timedelta(seconds=1.0)
ACTION_QUEUE_MAX_LENGTH = 1000


def event_monitor(app, backend):
    logger = logging.getLogger('swh.scheduler.listener')
    actions = {
        'last_send': utcnow() - 2*ACTION_SEND_DELAY,
        'queue': [],
    }

    def try_perform_actions(actions=actions):
        logger.debug('Try perform pending actions')
        if actions['queue'] and (
                len(actions['queue']) > ACTION_QUEUE_MAX_LENGTH or
                utcnow() - actions['last_send'] > ACTION_SEND_DELAY):
            perform_actions(actions)

    def perform_actions(actions, backend=backend):
        logger.info('Perform %s pending actions' % len(actions['queue']))
        action_map = {
            'start_task_run': backend.start_task_run,
            'end_task_run': backend.end_task_run,
        }

        messages = []
        db = backend.get_db()
        cursor = db.cursor(None)
        for action in actions['queue']:
            messages.append(action['message'])
            function = action_map[action['action']]
            args = action.get('args', ())
            kwargs = action.get('kwargs', {})
            kwargs['cur'] = cursor
            function(*args, **kwargs)

        db.commit()
        for message in messages:
            if not message.acknowledged:
                message.ack()
            else:
                logger.info('message already acknowledged: %s', message)
        actions['queue'] = []
        actions['last_send'] = utcnow()

    def queue_action(action, actions=actions):
        actions['queue'].append(action)
        try_perform_actions()

    def catchall_event(event, message):
        logger.info('event: %s, message:%s', event, message)
        if not message.acknowledged:
            message.ack()
        else:
            logger.info('message already acknowledged: %s', message)
        try_perform_actions()

    def task_started(event, message):
        logger.debug('task_started: event: %s' % event)
        logger.debug('task_started: message: %s' % message)

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
        logger.debug('task_succeeded: event: %s' % event)
        logger.debug('task_succeeded: message: %s' % message)
        result = event['result']

        logger.debug('task_succeeded: result: %s' % result)
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
        logger.debug('task_failed: event: %s' % event)
        logger.debug('task_failed: message: %s' % message)

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
        celery.current_app.connection(),
        app=celery.current_app,
        handlers={
            'task-started': task_started,
            'task-result': task_succeeded,
            'task-failed': task_failed,
            '*': catchall_event,
        },
        node_id='listener-%s' % socket.gethostname(),
    )

    errors = 0
    while True:
        try:
            recv.capture(limit=None, timeout=None, wakeup=True)
            errors = 0
        except KeyboardInterrupt:
            logger.exception('Keyboard interrupt, exiting')
            break
        except Exception:
            logger.exception('Unexpected exception')
            if errors < 5:
                time.sleep(errors)
                errors += 1
            else:
                logger.error('Too many consecutive errors, exiting')
                sys.exit(1)


@click.command()
@click.pass_context
def main(ctx):
    click.echo("Deprecated! Use 'swh-scheduler listener' instead.",
               err=True)
    ctx.exit(1)


if __name__ == '__main__':
    main()
