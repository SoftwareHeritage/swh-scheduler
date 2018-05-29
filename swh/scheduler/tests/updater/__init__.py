# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from arrow import utcnow

try:
    from hypothesis.strategies import from_regex
except ImportError:
    from hypothesis.strategies import text

    # Revert to using basic text generation
    def from_regex(*args, **kwargs):
        return text()


class UpdaterTestUtil:
    """Mixin intended for event generation purposes

    """
    def _make_event(self, event_type, name, origin_type):
        return {
            'type': event_type,
            'repo': {
                'name': name,
            },
            'created_at': utcnow(),
            'origin_type': origin_type,
        }

    def _make_events(self, events):
        for event_type, repo_name, origin_type in events:
            yield self._make_event(event_type, repo_name, origin_type)

    def _make_incomplete_event(self, event_type, name, origin_type,
                               missing_data_key):
        event = self._make_event(event_type, name, origin_type)
        del event[missing_data_key]
        return event

    def _make_incomplete_events(self, events):
        for event_type, repo_name, origin_type, missing_data_key in events:
            yield self._make_incomplete_event(event_type, repo_name,
                                              origin_type, missing_data_key)

    def _make_simple_event(self, event_type, name, origin_type):
        return {
            'type': event_type,
            'url': 'https://fakeurl/%s' % name,
            'origin_type': origin_type,
            'created_at': utcnow(),
        }
