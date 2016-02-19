# Copyright (C) 2015  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from .config import app as main_app
from ..backend import SchedulerBackend


def event_monitor(app, backend):
    state = app.events.State()

    def catchall_event(event, backend=backend):
        state.event(event)
        print(event)
        print(state)
        print(state.workers)

    def task_started(event, backend=backend):
        catchall_event(event, backend)
        backend.start_task_run(event['uuid'],
                               metadata={'worker': event['hostname']})

    def task_succeeded(event, backend=backend):
        catchall_event(event, backend)
        backend.end_task_run(event['uuid'],
                             eventful='True' in event['result'],
                             metadata={})

    def task_failed(event, backend=backend):
        catchall_event(event, backend)
        # backend.fail_task_run(event['uuid'])

    with app.connection() as connection:
        recv = app.events.Receiver(connection, handlers={
            'task-started': task_started,
            'task-succeeded': task_succeeded,
            'task-failed': task_failed,
            '*': catchall_event,
        })
        recv.capture(limit=None, timeout=None, wakeup=True)

if __name__ == '__main__':
    main_backend = SchedulerBackend()
    event_monitor(main_app, main_backend)
