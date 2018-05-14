# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


LISTENED_EVENTS = [
    'create',
    'delete',
    'public',
    'push'
]


class SWHEvent:
    """SWH's interesting event (resulting in an origin update)

    """
    def __init__(self, evt):
        self.event = evt

    def check(self):
        return 'type' in self.event and \
            self.event['type'].lower() in LISTENED_EVENTS

    def get(self):
        return {
            'type': self.event['type'],
            'url': self.event['url'],
            'last_seen': self.event.get('last_seen')
        }

    def __str__(self):
        return {
            'type': self.event['type'],
            'url': self.event['url'],
            'last_seen': self.event.get('last_seen')
        }
