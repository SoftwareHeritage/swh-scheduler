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
        return 'evt' in self.event and self.event['evt'] in LISTENED_EVENTS

    def __str__(self):
        return {
            'evt': self.event['evt'],
            'url': self.event['url']
        }


class SWHPublisher:
    def process(self):
        pass


class SWHSubscriber:
    def process(self):
        pass
