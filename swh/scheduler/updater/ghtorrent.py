# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import click
import json
import random
import string

from arrow import utcnow
from kombu import Connection, Exchange, Queue

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


class FakeRandomOriginGenerator:
    def _random_string(self, length):
        """Build a fake string of length length.

        """
        return ''.join([
            random.choice(string.ascii_letters + string.digits)
            for n in range(length)])

    def generate(self, user_range=range(5, 10), repo_range=range(10, 15)):
        """Build a fake url

        """
        length_username = random.choice(user_range)
        user = self._random_string(length_username)
        length_repo = random.choice(repo_range)
        repo = self._random_string(length_repo)
        return '%s/%s' % (user, repo)


class RabbitMQConn(SWHConfig):
    """RabbitMQ Connection class

    """
    CONFIG_BASE_FILENAME = 'backend/ghtorrent'

    DEFAULT_CONFIG = {
        'conn': ('dict', {
            'user': 'guest',
            'pass': 'guest',
            'port': 5672,
            'server': 'localhost',
            'exchange_name': 'ght-streams',
            'routing_key': 'something',
            'queue_name': 'fake-events'
        })
    }

    ADDITIONAL_CONFIG = {}

    def _connection_string(self):
        """Build the connection queue string.

        """
        return 'amqp://%s:%s@%s:%s' % (
            self.config['conn']['user'],
            self.config['conn']['pass'],
            self.config['conn']['server'],
            self.config['conn']['port']
        )

    def __init__(self, **config):
        super().__init__()
        if config:
            self.config = config
        else:
            self.config = self.parse_config_file(
                additional_configs=[self.ADDITIONAL_CONFIG])

        self.conn_string = self._connection_string()
        self.exchange = Exchange(self.config['conn']['exchange_name'],
                                 'topic', durable=True)
        self.routing_key = self.config['conn']['routing_key']
        self.queue = Queue(self.config['conn']['queue_name'],
                           exchange=self.exchange,
                           routing_key=self.routing_key,
                           auto_delete=True)


class FakeGHTorrentPublisher(RabbitMQConn):
    """Fake GHTorrent that randomly publishes fake events.  Those events
    are published in similar manner as described by ghtorrent's
    documentation [2].

    context: stuck with raw ghtorrent so far [1]

    [1] https://github.com/ghtorrent/ghtorrent.org/issues/397#issuecomment-387052462  # noqa
    [2] http://ghtorrent.org/streaming.html

    """

    ADDITIONAL_CONFIG = {
        'nb_messages': ('int', 100)
    }

    def __init__(self, **config):
        super().__init__(**config)
        self.fake_origin_generator = FakeRandomOriginGenerator()
        self.nb_messages = self.config['nb_messages']

    def _random_event(self):
        """Create a fake and random event

        """
        event_type = random.choice(['evt', 'ent'])
        sub_event = random.choice(events[event_type])
        return {
            'type': sub_event,
            'repo': {
                'name': self.fake_origin_generator.generate(),
            },
            'created_at': utcnow().isoformat()

        }

    def publish(self, nb_messages=None):
        if not nb_messages:
            nb_messages = self.nb_messages

        with Connection(self.conn_string) as conn:
            with conn.Producer(serializer='json') as producer:
                for n in range(nb_messages):
                    event = self._random_event()
                    producer.publish(event,
                                     exchange=self.exchange,
                                     routing_key=self.routing_key,
                                     declare=[self.queue])


def convert_event(event):
    """Given ghtorrent event, convert it to an swhevent instance.

    """
    if isinstance(event, str):
        event = json.loads(event)

    keys = ['type', 'repo', 'created_at']
    for k in keys:
        if k not in event:
            raise ValueError('Event should have the \'%s\' entry defined' % k)

    _type = event['type'].lower().rstrip('Event')
    _repo_name = 'https://github.com/%s' % event['repo']['name']

    return SWHEvent({
        'type': _type,
        'url': _repo_name,
        'last_seen': event['created_at']
    })


class GHTorrentConsumer(RabbitMQConn, UpdaterConsumer):
    """GHTorrent consumer

    """
    ADDITIONAL_CONFIG = {
        'debug': ('bool', False),
        'batch': ('int', 1000),
    }

    def __init__(self, **config):
        super().__init__(**config)
        self.debug = self.config['debug']
        self.batch = self.config['batch']

    def post_process_message(self, message):
        """Acknowledge the read message.

        """
        message.ack()

    def convert_event(self, event):
        """Given ghtorrent event, convert it to an swhevent instance.

        """
        return convert_event(event)

    def consume(self):
        """Open a rabbitmq connection and consumes events from that endpoint.

        """
        with Connection(self.conn_string) as conn:
            with conn.Consumer(self.queue, callbacks=[self.process_message],
                               auto_declare=True):
                while True:
                    conn.drain_events()


@click.command()
def main():
    """Consume events from ghtorrent

    """
    GHTorrentConsumer().consume()


if __name__ == '__main__':
    main()
