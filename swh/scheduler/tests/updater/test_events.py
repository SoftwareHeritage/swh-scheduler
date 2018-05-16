# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from arrow import utcnow
from hypothesis import given
from hypothesis.strategies import text, sampled_from
from nose.tools import istest

from swh.scheduler.updater.events import SWHEvent, LISTENED_EVENTS
from swh.scheduler.updater.ghtorrent import events


def event_values_ko():
    return set(events['evt']).union(
        set(events['ent'])).difference(
        set(LISTENED_EVENTS))


WRONG_EVENTS = sorted(list(event_values_ko()))


class EventTest(unittest.TestCase):
    def _make_event(self, event_name):
        return {
            'type': event_name,
            'url': 'something',
            'last_seen': utcnow(),
        }

    @istest
    @given(sampled_from(LISTENED_EVENTS))
    def is_interesting_ok(self, event_name):
        evt = self._make_event(event_name)
        self.assertTrue(SWHEvent(evt).is_interesting())

    @istest
    @given(text())
    def is_interested_with_noisy_event_should_be_ko(self, event_name):
        if event_name in LISTENED_EVENTS:
            # just in generation generates a real and correct name, skip it
            return
        evt = self._make_event(event_name)
        self.assertFalse(SWHEvent(evt).is_interesting())

    @istest
    @given(sampled_from(WRONG_EVENTS))
    def is_interesting_ko(self, event_name):
        evt = self._make_event(event_name)
        self.assertFalse(SWHEvent(evt).is_interesting())
