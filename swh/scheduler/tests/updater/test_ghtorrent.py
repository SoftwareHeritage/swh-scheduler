# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from unittest.mock import patch

from hypothesis import given
from hypothesis.strategies import sampled_from

from swh.scheduler.updater.events import SWHEvent
from swh.scheduler.updater.ghtorrent import (INTERESTING_EVENT_KEYS,
                                             GHTorrentConsumer, events)
from swh.scheduler.updater.backend import SchedulerUpdaterBackend

from . import UpdaterTestUtil, from_regex


def event_values():
    return set(events['evt']).union(set(events['ent']))


def ghtorrentize_event_name(event_name):
    return '%sEvent' % event_name.capitalize()


EVENT_TYPES = sorted([ghtorrentize_event_name(e) for e in event_values()])


class FakeChannel:
    """Fake Channel (virtual connection inside a connection)

    """
    def close(self):
        self.close = True


class FakeConnection:
    """Fake Rabbitmq connection for test purposes

    """
    def __init__(self, conn_string):
        self._conn_string = conn_string
        self._connect = False
        self._release = False
        self._channel = False

    def connect(self):
        self._connect = True
        return True

    def release(self):
        self._connect = False
        self._release = True

    def channel(self):
        self._channel = True
        return FakeChannel()


class GHTorrentConsumerTest(UpdaterTestUtil, unittest.TestCase):
    def setUp(self):
        config = {
            'ghtorrent': {
                'rabbitmq': {
                    'conn': {
                        'url': 'amqp://u:p@https://somewhere:9807',
                    },
                    'prefetch_read': 17,
                },
                'batch_cache_write': 42,
            },
            'scheduler_updater': {
                'cls': 'local',
                'args': {
                    'db': 'dbname=softwareheritage-scheduler-updater-dev',
                },
            },
        }

        GHTorrentConsumer.connection_class = FakeConnection
        with patch.object(
                SchedulerUpdaterBackend, '__init__', return_value=None):
            self.consumer = GHTorrentConsumer(**config)

    @patch('swh.scheduler.updater.backend.SchedulerUpdaterBackend')
    def test_init(self, mock_backend):
        # given
        # check init is ok
        self.assertEqual(self.consumer.batch, 42)
        self.assertEqual(self.consumer.prefetch_read, 17)

    def test_has_events(self):
        self.assertTrue(self.consumer.has_events())

    def test_connection(self):
        # when
        self.consumer.open_connection()

        # then
        self.assertEqual(self.consumer.conn._conn_string,
                         'amqp://u:p@https://somewhere:9807')
        self.assertTrue(self.consumer.conn._connect)
        self.assertFalse(self.consumer.conn._release)

        # when
        self.consumer.close_connection()

        # then
        self.assertFalse(self.consumer.conn._connect)
        self.assertTrue(self.consumer.conn._release)
        self.assertIsInstance(self.consumer.channel, FakeChannel)

    @given(sampled_from(EVENT_TYPES),
           from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$'))
    def test_convert_event_ok(self, event_type, name):
        input_event = self._make_event(event_type, name, 'git')
        actual_event = self.consumer.convert_event(input_event)

        self.assertTrue(isinstance(actual_event, SWHEvent))

        event = actual_event.get()

        expected_event = {
            'type': event_type.lower().rstrip('Event'),
            'url': 'https://github.com/%s' % name,
            'last_seen': input_event['created_at'],
            'cnt': 1,
            'origin_type': 'git',
        }
        self.assertEqual(event, expected_event)

    @given(sampled_from(EVENT_TYPES),
           from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$'),
           sampled_from(INTERESTING_EVENT_KEYS))
    def test_convert_event_ko(self, event_type, name, missing_data_key):
        input_event = self._make_incomplete_event(
            event_type, name, 'git', missing_data_key)

        logger = self.consumer.log
        del self.consumer.log  # prevent gazillions of warnings
        actual_converted_event = self.consumer.convert_event(input_event)
        self.consumer.log = logger
        self.assertIsNone(actual_converted_event)

    @patch('swh.scheduler.updater.ghtorrent.collect_replies')
    def test_consume_events(self, mock_collect_replies):
        # given
        self.consumer.queue = 'fake-queue'  # hack
        self.consumer.open_connection()

        fake_events = [
            self._make_event('PushEvent', 'user/some-repo', 'git'),
            self._make_event('PushEvent', 'user2/some-other-repo', 'git'),
        ]

        mock_collect_replies.return_value = fake_events

        # when
        actual_events = []
        for e in self.consumer.consume_events():
            actual_events.append(e)

        # then
        self.assertEqual(fake_events, actual_events)

        mock_collect_replies.assert_called_once_with(
            self.consumer.conn,
            self.consumer.channel,
            'fake-queue',
            no_ack=False,
            limit=17
        )
