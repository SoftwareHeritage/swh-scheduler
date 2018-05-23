# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


LISTENED_EVENTS = [
    'delete',
    'public',
    'push'
]


class SWHEvent:
    """SWH's interesting event (resulting in an origin update)

    """
    def __init__(self, evt, cnt=1):
        self.event = evt
        self.type = evt['type'].lower()
        self.url = evt['url']
        self.last_seen = evt.get('last_seen')
        self.cnt = cnt
        self.origin_type = evt.get('origin_type')

    def is_interesting(self):
        return self.type in LISTENED_EVENTS

    def get(self):
        return {
            'type': self.type,
            'url': self.url,
            'last_seen': self.last_seen,
            'cnt': self.cnt,
            'origin_type': self.origin_type
        }

    def __str__(self):
        return str(self.get())
