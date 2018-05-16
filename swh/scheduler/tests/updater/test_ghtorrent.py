# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from arrow import utcnow
from nose.plugins.attrib import attr
from nose.tools import istest
from hypothesis import given
from hypothesis.strategies import sampled_from, from_regex

from swh.scheduler.updater.events import SWHEvent
from swh.scheduler.updater.ghtorrent import events, convert_event


def event_values():
    return set(events['evt']).union(set(events['ent']))


def ghtorrentize_event_name(event_name):
    return '%sEvent' % event_name.capitalize()


EVENT_TYPES = sorted([ghtorrentize_event_name(e) for e in event_values()])


@attr('db')
class GHTorrentTest(unittest.TestCase):
    def _make_event(self, event_type, name):
        return {
            'type': event_type,
            'repo': {
                'name': name,
            },
            'created_at': utcnow(),
        }

    @istest
    @given(sampled_from(EVENT_TYPES),
           from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$'))
    def convert_event_ok(self, event_type, name):
        input_event = self._make_event(event_type, name)
        actual_event = convert_event(input_event)

        self.assertTrue(isinstance(actual_event, SWHEvent))

        event = actual_event.get()

        expected_event = {
            'type': event_type.lower().rstrip('Event'),
            'url': 'https://github.com/%s' % name,
            'last_seen': input_event['created_at'],
        }
        self.assertEqual(event, expected_event)

    def _make_incomplete_event(self, event_type, name, missing_data_key):
        event = self._make_event(event_type, name)
        del event[missing_data_key]
        return event

    @istest
    @given(sampled_from(EVENT_TYPES),
           from_regex(r'^[a-z0-9]{5,7}/[a-z0-9]{3,10}$'),
           sampled_from(['type', 'repo', 'created_at']))
    def convert_event_ko(self, event_type, name, missing_data_key):
        input_event = self._make_incomplete_event(
            event_type, name, missing_data_key)

        with self.assertRaisesRegex(
                ValueError,
                'Event should have the \'%s\' entry defined' % (
                    missing_data_key, )):
            convert_event(input_event)
