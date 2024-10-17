# Copyright (C) 2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from datetime import timedelta
import logging
from typing import Tuple

from swh.scheduler.model import ListedOrigin, OriginVisitStats
from swh.scheduler.tests.test_cli import invoke as basic_invoke
from swh.scheduler.utils import utcnow

MODULE_NAMES = [
    "swh.scheduler.cli.admin",
    "swh.scheduler.celery_backend.first_visits",
    "swh.scheduler.celery_backend.utils",
]


def invoke(
    scheduler, args: Tuple[str, ...] = (), catch_exceptions: bool = False, **kwargs
):
    """Invoke swh cli and returns the result call."""
    return basic_invoke(
        scheduler, args=list(args), catch_exceptions=catch_exceptions, **kwargs
    )


def set_log_level(caplog, module_names):
    """Configure the caplog fixtures"""
    for module_name in module_names:
        caplog.set_level(logging.INFO, logger=module_name)


def test_runner_high_priority_first_visits(
    swh_scheduler,
    swh_scheduler_celery_app,
    visit_types,
    task_types,
    mocker,
    caplog,
):
    set_log_level(caplog, MODULE_NAMES)

    nb_origins_per_visit_type = 10
    # number of celery queue slots per visit type is lower than the
    # number of listed origins for that visit type
    nb_available_queue_slots = nb_origins_per_visit_type // 2

    mocker.patch.object(swh_scheduler_celery_app, "send_task")
    # mock number of available slots in queues to a fixed value
    mocker.patch(
        "swh.scheduler.celery_backend.config.get_available_slots"
    ).return_value = nb_available_queue_slots

    # create a lister with high priority first visits
    lister_name = "save-bulk"
    lister_instance_name = "foo"
    lister = swh_scheduler.get_or_create_lister(
        name=lister_name,
        instance_name=lister_instance_name,
        first_visits_queue_prefix="save_bulk",
    )
    # register origins for the lister
    for visit_type in visit_types:
        listed_origins = [
            ListedOrigin(
                lister_id=lister.id,
                url=f"https://{visit_type}.example.org/project{i}",
                visit_type=visit_type,
            )
            for i in range(nb_origins_per_visit_type)
        ]
        swh_scheduler.record_listed_origins(listed_origins)
        # mark some origins visits already scheduled in the past to
        # check they are scheduled again by the tested command
        swh_scheduler.origin_visit_stats_upsert(
            [
                OriginVisitStats(
                    url=listed_origin.url,
                    visit_type=listed_origin.visit_type,
                    last_scheduled=utcnow() - timedelta(days=180),
                )
                for i, listed_origin in enumerate(listed_origins)
                if i % 2 == 1
            ]
        )

    # mark listing as finished
    lister = lister.evolve(last_listing_finished_at=utcnow())
    lister = swh_scheduler.update_lister(lister)

    # start scheduling first visits
    result = invoke(swh_scheduler, args=("start-runner-first-visits",))
    assert result.exit_code == 0

    records = [record.message for record in caplog.records]

    # check expected number of visits were scheduled
    for visit_type in visit_types:
        assert (
            f"{nb_available_queue_slots} visits of type {visit_type} to send to celery"
            in records
        )

    # check there is still origin first visits to schedule
    assert (
        f"All first visits of origins registered by lister with name '{lister_name}' "
        f"and instance '{lister_instance_name}' were scheduled.'"
    ) not in records

    lister = swh_scheduler.get_lister(
        name=lister_name, instance_name=lister_instance_name
    )
    assert lister.first_visits_scheduled_at is None

    # Reset the logs for the next cli call
    caplog.clear()

    # continue scheduling first visits
    result = invoke(swh_scheduler, args=("start-runner-first-visits",))

    assert result.exit_code == 0
    records = [record.message for record in caplog.records]

    # check expected number of visits were scheduled
    for visit_type in visit_types:
        assert (
            f"{nb_available_queue_slots} visits of type {visit_type} to send to celery"
            in records
        )
    # check all listed origins first visits were scheduled
    assert (
        f"All first visits of origins registered by lister with name '{lister_name}' "
        f"and instance '{lister_instance_name}' were scheduled.'"
    ) in records

    lister = swh_scheduler.get_lister(
        name=lister_name, instance_name=lister_instance_name
    )
    assert lister.first_visits_scheduled_at is not None
