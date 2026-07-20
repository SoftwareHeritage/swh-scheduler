# Copyright (C) 2026  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import pytest

from swh.scheduler.celery_backend.pika_listener import process_event
from swh.scheduler.utils import create_oneshot_task, utcnow


@pytest.mark.parametrize(
    "error_message",
    [
        "aborting due to possible repository corruption on the remote side.",
        "aborting due to possible repository corruption on the remote side.\x00",
    ],
)
def test_pika_listener_process_event(swh_scheduler, task_types, error_message):
    backend_id = "c83ec791-db82-479c-a37e-9e7a1befde8a"
    task_started_event = {
        "hostname": "loader@90c75b081226",
        "utcoffset": 0,
        "pid": 1,
        "clock": 28,
        "uuid": backend_id,
        "timestamp": 1784540900.321152,
        "type": "task-started",
    }

    task_result_event = {
        "hostname": "loader@90c75b081226",
        "utcoffset": 0,
        "pid": 184,
        "clock": 1,
        "uuid": backend_id,
        "result": {
            "status": "failed",
            "error": error_message,
        },
        "timestamp": 1784540901.5753186,
        "type": "task-result",
    }

    task = create_oneshot_task(
        task_types["load-test-git"].type, url="https://example.com/user/project.git"
    )
    task = swh_scheduler.create_tasks([task])[0]
    swh_scheduler.schedule_task_run(
        task_id=task.id, backend_id=backend_id, timestamp=utcnow()
    )

    process_event(task_started_event, swh_scheduler)
    process_event(task_result_event, swh_scheduler)

    task_run = swh_scheduler.get_task_runs([task.id])[0]

    assert (
        task_run.metadata["error"]
        == "aborting due to possible repository corruption on the remote side."
    )
