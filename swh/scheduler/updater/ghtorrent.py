# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import string

from kombu import Connection, Exchange, Queue


events = {
    # ghtorrent events related to github events (that's the one we are
    # interested in)
    'evt': [
        'commitcomment', 'create', 'delete', 'deployment',
        'deploymentstatus', 'download', 'follow', 'fork', 'forkapply',
        'gist', 'gollum', 'issuecomment', 'issues', 'member',
        'membership', 'pagebuild', 'public', 'pullrequest',
        'pullrequestreviewcomment', 'push', 'release', 'repository',
        'status', 'teamadd', 'watch'
    ],
    # ghtorrent events related to mongodb insert
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
        return 'https://fake.github.com/%s/%s' % (user, repo)


class RabbitMQConn:
    DEFAULT_CONFIG = {
        'user': 'guest',
        'pass': 'guest',
        'port': 5672,
        'server': 'localhost',
        'exchange_name': 'ght-streams',
        'routing_key': 'something',
        'queue_name': 'fake-events'
    }

    def _conn_queue(self, config):
        """Build the connection queue string.

        """
        return 'amqp://%s:%s@%s:%s' % (
            self.config['user'],
            self.config['pass'],
            self.config['server'],
            self.config['port']
        )

    def __init__(self, **config):
        if config:
            self.config = config
        else:
            self.config = self.DEFAULT_CONFIG

        self.conn_queue = self._conn_queue(self.config)
        self.exchange = Exchange(self.config['exchange_name'],
                                 'topic', durable=True)
        self.queue = Queue(self.config['queue_name'],
                           exchange=self.exchange,
                           routing_key=self.config['routing_key'],
                           auto_delete=True)


class FakeGHTorrentPublisher(RabbitMQConn):
    """Fake GHTorrent that randomly publishes fake events.  Those events
    are published in similar manner as described by ghtorrent's
    documentation [2].

    context: stuck with raw ghtorrent so far [1]

    [1] https://github.com/ghtorrent/ghtorrent.org/issues/397#issuecomment-387052462  # noqa
    [2] http://ghtorrent.org/streaming.html

    """
    def __init__(self, **config):
        super().__init__(**config)
        self.fake_origin_generator = FakeRandomOriginGenerator()

    def _random_event(self):
        """Create a fake and random event

        """
        event_type = random.choice(['evt', 'ent'])
        sub_event = random.choice(events[event_type])
        return {
            'event': sub_event,
            'url': self.fake_origin_generator.generate(),
        }

    def publish(self, nb_messages=10):
        with Connection(self.conn_queue) as conn:
            with conn.Producer(serializer='json') as producer:
                for n in range(nb_messages):
                    event = self._random_event()
                    producer.publish(event,
                                     exchange=self.exchange,
                                     routing_key=self.config['routing_key'],
                                     declare=[self.queue])


def process_message(body, message):
    print('####', body, message)
    message.ack()


class GHTorrentConsumer(RabbitMQConn):
    """GHTorrent consumer

    """
    def consume(self):
        with Connection(self.conn_queue) as conn:
            with conn.Consumer(self.queue, callbacks=[process_message],
                               auto_declare=True) as consumer:
                consumer.consume()
