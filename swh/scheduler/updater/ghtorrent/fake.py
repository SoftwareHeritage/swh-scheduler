# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import random
import string

from arrow import utcnow
from kombu import Connection

from swh.scheduler.updater.ghtorrent import RabbitMQConn, events


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

        with Connection(self.config['conn']['url']) as conn:
            with conn.Producer(serializer='json') as producer:
                for n in range(nb_messages):
                    event = self._random_event()
                    producer.publish(event,
                                     exchange=self.exchange,
                                     routing_key=self.routing_key,
                                     declare=[self.queue])
