# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import json

from kombu import Connection, Exchange, Queue
from kombu.common import collect_replies

from swh.core.config import SWHConfig
from swh.scheduler.updater.events import SWHEvent
from swh.scheduler.updater.consumer import UpdaterConsumer


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


class RabbitMQConn(SWHConfig):
    """RabbitMQ Connection class

    """
    CONFIG_BASE_FILENAME = 'backend/ghtorrent'

    DEFAULT_CONFIG = {
        'conn': ('dict', {
            'url': 'amqp://guest:guest@localhost:5672',
            'exchange_name': 'ght-streams',
            'routing_key': 'something',
            'queue_name': 'fake-events'
        })
    }

    ADDITIONAL_CONFIG = {}

    def __init__(self, **config):
        super().__init__()
        if config and set(config.keys()) - {'log_class'} != set():
            self.config = config
        else:
            self.config = self.parse_config_file(
                additional_configs=[self.ADDITIONAL_CONFIG])

        self.conn_string = self.config['conn']['url']
        self.exchange = Exchange(self.config['conn']['exchange_name'],
                                 'topic', durable=True)
        self.routing_key = self.config['conn']['routing_key']
        self.queue = Queue(self.config['conn']['queue_name'],
                           exchange=self.exchange,
                           routing_key=self.routing_key,
                           auto_delete=True)


INTERESTING_EVENT_KEYS = ['type', 'repo', 'created_at']


class GHTorrentConsumer(RabbitMQConn, UpdaterConsumer):
    """GHTorrent events consumer

    """
    ADDITIONAL_CONFIG = {
        'debug': ('bool', False),
        'batch_cache_write': ('int', 1000),
        'rabbitmq_prefetch_read': ('int', 100),
    }

    def __init__(self, config=None, _connection_class=Connection):
        if config is None:
            super().__init__(
                log_class='swh.scheduler.updater.ghtorrent.GHTorrentConsumer')
        else:
            self.config = config
        self._connection_class = _connection_class
        self.debug = self.config['debug']
        self.batch = self.config['batch_cache_write']
        self.prefetch_read = self.config['rabbitmq_prefetch_read']

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
                    self.log.warn(
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
        self.conn = self._connection_class(self.config['conn']['url'])
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
