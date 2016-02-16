# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .config import app as main_app


def event_monitor(app):
    state = app.events.State()

    def announce_event(event):
        state.event(event)
        print(event)
        print(state)
        print(state.workers)

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
                '*': announce_event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    event_monitor(main_app)
