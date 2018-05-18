# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from arrow import utcnow


class UpdaterTestUtil:
    """Mixin intended for event generation purposes

    """
    def _make_event(self, event_type, name):
        return {
            'type': event_type,
            'repo': {
                'name': name,
            },
            'created_at': utcnow(),
        }

    def _make_events(self, events):
        for event_type, repo_name in events:
            yield self._make_event(event_type, repo_name)

    def _make_incomplete_event(self, event_type, name, missing_data_key):
        event = self._make_event(event_type, name)
        del event[missing_data_key]
        return event

    def _make_incomplete_events(self, events):
        for event_type, repo_name, missing_data_key in events:
            yield self._make_incomplete_event(event_type, repo_name,
                                              missing_data_key)
