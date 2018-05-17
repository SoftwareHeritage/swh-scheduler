# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from arrow import utcnow
from hypothesis import given
from hypothesis.strategies import sampled_from, from_regex, lists, tuples, text
from nose.tools import istest

from swh.scheduler.updater.events import SWHEvent, LISTENED_EVENTS
from swh.scheduler.updater.consumer import UpdaterConsumer


class FakeSchedulerUpdaterBackend:
    def __init__(self):
        self.events = []

    def cache_put(self, events):
        self.events.append(events)


class FakeUpdaterConsumerBase(UpdaterConsumer):
    def __init__(self, backend_class=FakeSchedulerUpdaterBackend):
        super().__init__(backend_class=backend_class)
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
        self.updater = FakeUpdaterConsumerRaise()

    @istest
    def running_raise(self):
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
        self.updater = FakeUpdaterConsumerNoEvent()

    @istest
    def running_does_not_consume(self):
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


class FakeUpdaterConsumer(FakeUpdaterConsumerBase):
    def __init__(self, messages):
        super().__init__()
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

    def convert_event(self, event):
        e = {
            'type': event['type'],
            'url': 'https://fake.url/%s' % event['name'],
            'last_seen': event['created_at']
        }
        return SWHEvent(e)


class UpdaterConsumerWithEventTest(unittest.TestCase):
    def _make_event(self, event_type, name):
        return {
            'type': event_type,
            'name': name,
            'created_at': utcnow(),
        }

    def _make_events(self, events):
        for event_type, repo_name in events:
            yield self._make_event(event_type, repo_name)

    @istest
    @given(lists(tuples(text(),
                 from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$')),
                 min_size=3, max_size=10),
           lists(tuples(sampled_from(LISTENED_EVENTS),
                 from_regex(r'^[a-z0-9]{5,10}/[a-z0-9]{7,12}$')),
                 min_size=3, max_size=10))
    def running_with_uninteresting_events(self, uninteresting_events, events):
        """Interesting events are written to cache, dropping uninteresting ones

        """
        # given
        all_events = events.copy()
        all_events.extend(uninteresting_events)
        updater = FakeUpdaterConsumer(list(self._make_events(all_events)))

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
        # uninteresting_events were dropped
        self.assertTrue(len(updater.backend.events), len(events))
