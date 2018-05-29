# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from hypothesis import given
from hypothesis.strategies import text, sampled_from
from nose.tools import istest

from swh.scheduler.updater.events import SWHEvent, LISTENED_EVENTS
from swh.scheduler.updater.ghtorrent import events

from . import UpdaterTestUtil


def event_values_ko():
    return set(events['evt']).union(
        set(events['ent'])).difference(
        set(LISTENED_EVENTS))


WRONG_EVENTS = sorted(list(event_values_ko()))


class EventTest(UpdaterTestUtil, unittest.TestCase):
    @istest
    @given(sampled_from(LISTENED_EVENTS), text(), text())
    def is_interesting_ok(self, event_type, name, origin_type):
        evt = self._make_simple_event(event_type, name, origin_type)
        self.assertTrue(SWHEvent(evt).is_interesting())

    @istest
    @given(text(), text(), text())
    def is_interested_with_noisy_event_should_be_ko(
            self, event_type, name, origin_type):
        if event_type in LISTENED_EVENTS:
            # just in case something good is generated, skip it
            return
        evt = self._make_simple_event(event_type, name, origin_type)
        self.assertFalse(SWHEvent(evt).is_interesting())

    @istest
    @given(sampled_from(WRONG_EVENTS), text(), text())
    def is_interesting_ko(self, event_type, name, origin_type):
        evt = self._make_simple_event(event_type, name, origin_type)
        self.assertFalse(SWHEvent(evt).is_interesting())
