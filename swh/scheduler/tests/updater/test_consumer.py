# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest
from itertools import chain

from hypothesis import given, settings, HealthCheck
from hypothesis.strategies import lists, sampled_from, text, tuples

from swh.scheduler.updater.consumer import UpdaterConsumer
from swh.scheduler.updater.events import LISTENED_EVENTS, SWHEvent

from . import UpdaterTestUtil, from_regex


class FakeSchedulerUpdaterBackend:
    def __init__(self):
        self.events = []

    def cache_put(self, events):
        self.events.append(events)


class FakeUpdaterConsumerBase(UpdaterConsumer):
    def __init__(self, backend):
        super().__init__(backend)
        self.connection_opened = False
        self.connection_closed = False
        self.consume_called = False
        self.has_events_called = False

    def open_connection(self):
        self.connection_opened = True

    def close_connection(self):
        self.connection_closed = True

    def convert_event(self, event):
        pass


class FakeUpdaterConsumerRaise(FakeUpdaterConsumerBase):
    def has_events(self):
        self.has_events_called = True
        return True

    def consume_events(self):
        self.consume_called = True
        raise ValueError('Broken stuff')


class UpdaterConsumerRaisingTest(unittest.TestCase):
    def setUp(self):
        self.updater = FakeUpdaterConsumerRaise(
            FakeSchedulerUpdaterBackend())

    def test_running_raise(self):
        """Raising during run should finish fine.

        """
        # given
        self.assertEqual(self.updater.count, 0)
        self.assertEqual(self.updater.seen_events, set())
        self.assertEqual(self.updater.events, [])

        # when
        with self.assertRaisesRegex(ValueError, 'Broken stuff'):
            self.updater.run()

        # then
        self.assertEqual(self.updater.count, 0)
        self.assertEqual(self.updater.seen_events, set())
        self.assertEqual(self.updater.events, [])
        self.assertTrue(self.updater.connection_opened)
        self.assertTrue(self.updater.has_events_called)
        self.assertTrue(self.updater.connection_closed)
        self.assertTrue(self.updater.consume_called)


class FakeUpdaterConsumerNoEvent(FakeUpdaterConsumerBase):
    def has_events(self):
        self.has_events_called = True
        return False

    def consume_events(self):
        self.consume_called = True


class UpdaterConsumerNoEventTest(unittest.TestCase):
    def setUp(self):
        self.updater = FakeUpdaterConsumerNoEvent(
            FakeSchedulerUpdaterBackend())

    def test_running_does_not_consume(self):
        """Run with no events should do just fine"""
        # given
        self.assertEqual(self.updater.count, 0)
        self.assertEqual(self.updater.seen_events, set())
        self.assertEqual(self.updater.events, [])

        # when
        self.updater.run()

        # then
        self.assertEqual(self.updater.count, 0)
        self.assertEqual(self.updater.seen_events, set())
        self.assertEqual(self.updater.events, [])
        self.assertTrue(self.updater.connection_opened)
        self.assertTrue(self.updater.has_events_called)
        self.assertTrue(self.updater.connection_closed)
        self.assertFalse(self.updater.consume_called)


EVENT_KEYS = ['type', 'repo', 'created_at', 'origin_type']


class FakeUpdaterConsumer(FakeUpdaterConsumerBase):
    def __init__(self, backend, messages):
        super().__init__(backend)
        self.messages = messages
        self.debug = False

    def has_events(self):
        self.has_events_called = True
        return len(self.messages) > 0

    def consume_events(self):
        self.consume_called = True
        for msg in self.messages:
            yield msg
            self.messages.pop()

    def convert_event(self, event, keys=EVENT_KEYS):
        for k in keys:
            v = event.get(k)
            if v is None:
                return None

        e = {
            'type': event['type'],
            'url': 'https://fake.url/%s' % event['repo']['name'],
            'last_seen': event['created_at'],
            'origin_type': event['origin_type'],
        }
        return SWHEvent(e)


class UpdaterConsumerWithEventTest(UpdaterTestUtil, unittest.TestCase):
    @settings(suppress_health_check=[HealthCheck.too_slow])
    @given(lists(tuples(sampled_from(LISTENED_EVENTS),  # event type
                        from_regex(r'^[a-z0-9]{5,10}/[a-z0-9]{7,12}$'),  # name
                        text()),                        # origin type
                 min_size=3, max_size=10),
           lists(tuples(text(),                         # event type
                        from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$'),  # name
                        text()),                        # origin type
                 min_size=3, max_size=10),
           lists(tuples(sampled_from(LISTENED_EVENTS),  # event type
                        from_regex(r'^[a-z0-9]{5,10}/[a-z0-9]{7,12}$'),  # name
                        text(),                     # origin type
                        sampled_from(EVENT_KEYS)),  # keys to drop
                 min_size=3, max_size=10))
    def test_running(self, events, uninteresting_events, incomplete_events):
        """Interesting events are written to cache, others are dropped

        """
        # given
        ready_events = self._make_events(events)
        ready_uninteresting_events = self._make_events(uninteresting_events)
        ready_incomplete_events = self._make_incomplete_events(
            incomplete_events)

        updater = FakeUpdaterConsumer(
            FakeSchedulerUpdaterBackend(),
            list(chain(
                ready_events, ready_incomplete_events,
                ready_uninteresting_events)))

        self.assertEqual(updater.count, 0)
        self.assertEqual(updater.seen_events, set())
        self.assertEqual(updater.events, [])

        # when
        updater.run()

        # then
        self.assertEqual(updater.count, 0)
        self.assertEqual(updater.seen_events, set())
        self.assertEqual(updater.events, [])
        self.assertTrue(updater.connection_opened)
        self.assertTrue(updater.has_events_called)
        self.assertTrue(updater.connection_closed)
        self.assertTrue(updater.consume_called)

        self.assertEqual(updater.messages, [])
        # uninteresting or incomplete events are dropped
        self.assertTrue(len(updater.backend.events), len(events))
