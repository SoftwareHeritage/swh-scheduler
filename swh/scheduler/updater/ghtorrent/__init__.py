# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from kombu import Connection, Exchange, Queue
from kombu.common import collect_replies

from swh.core.config import merge_configs

from swh.scheduler.updater.events import SWHEvent
from swh.scheduler.updater.consumer import UpdaterConsumer
from swh.scheduler.updater.backend import SchedulerUpdaterBackend


events = {
    # ghtorrent events related to github events (interesting)
    'evt': [
        'commitcomment', 'create', 'delete', 'deployment',
        'deploymentstatus', 'download', 'follow', 'fork', 'forkapply',
        'gist', 'gollum', 'issuecomment', 'issues', 'member',
        'membership', 'pagebuild', 'public', 'pullrequest',
        'pullrequestreviewcomment', 'push', 'release', 'repository',
        'status', 'teamadd', 'watch'
    ],
    # ghtorrent events related to mongodb insert (not interesting)
    'ent': [
        'commit_comments', 'commits', 'followers', 'forks',
        'geo_cache', 'issue_comments', 'issue_events', 'issues',
        'org_members', 'pull_request_comments', 'pull_requests',
        'repo_collaborators', 'repo_labels', 'repos', 'users', 'watchers'
    ]
}

INTERESTING_EVENT_KEYS = ['type', 'repo', 'created_at']

DEFAULT_CONFIG = {
    'ghtorrent': {
        'batch_cache_write': 1000,
        'rabbitmq': {
            'prefetch_read': 100,
            'conn': {
                'url': 'amqp://guest:guest@localhost:5672',
                'exchange_name': 'ght-streams',
                'routing_key': 'something',
                'queue_name': 'fake-events',
            },
        },
    },
    'scheduler_updater': {
        'cls': 'local',
        'args': {
            'db': 'dbname=softwareheritage-scheduler-updater-dev',
            'cache_read_limit': 1000,
        },
    },
}


class GHTorrentConsumer(UpdaterConsumer):
    """GHTorrent events consumer

    """
    connection_class = Connection

    def __init__(self, **config):
        self.config = merge_configs(DEFAULT_CONFIG, config)

        ght_config = self.config['ghtorrent']
        rmq_config = ght_config['rabbitmq']
        self.prefetch_read = int(rmq_config.get('prefetch_read', 100))

        exchange = Exchange(
            rmq_config['conn']['exchange_name'],
            'topic', durable=True)
        routing_key = rmq_config['conn']['routing_key']
        self.queue = Queue(rmq_config['conn']['queue_name'],
                           exchange=exchange,
                           routing_key=routing_key,
                           auto_delete=True)

        if self.config['scheduler_updater']['cls'] != 'local':
            raise ValueError(
                'The scheduler_updater can only be a cls=local for now')
        backend = SchedulerUpdaterBackend(
            **self.config['scheduler_updater']['args'])

        super().__init__(backend, ght_config.get('batch_cache_write', 1000))

    def has_events(self):
        """Always has events

        """
        return True

    def convert_event(self, event):
        """Given ghtorrent event, convert it to a SWHEvent instance.

        """
        if isinstance(event, str):
            event = json.loads(event)
        for k in INTERESTING_EVENT_KEYS:
            if k not in event:
                if hasattr(self, 'log'):
                    self.log.warning(
                        'Event should have the \'%s\' entry defined' % k)
                return None

        _type = event['type'].lower().rstrip('Event')
        _repo_name = 'https://github.com/%s' % event['repo']['name']

        return SWHEvent({
            'type': _type,
            'url': _repo_name,
            'last_seen': event['created_at'],
            'origin_type': 'git',
        })

    def open_connection(self):
        """Open rabbitmq connection

        """
        self.conn = self.connection_class(
            self.config['ghtorrent']['rabbitmq']['conn']['url'])
        self.conn.connect()
        self.channel = self.conn.channel()

    def close_connection(self):
        """Close rabbitmq connection

        """
        self.channel.close()
        self.conn.release()

    def consume_events(self):
        """Consume and yield queue messages

        """
        yield from collect_replies(
            self.conn, self.channel, self.queue,
            no_ack=False, limit=self.prefetch_read)
