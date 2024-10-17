# Copyright (C) 2024 The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from swh.scheduler.interface import SchedulerInterface


logger = logging.getLogger(__name__)


def schedule_first_visits(backend: SchedulerInterface):
    """Schedule first visits with high priority for origins registered by listers
    having the first_visits_priority_queue attribute set.

    """
    from functools import partial

    from swh.core.api.classes import stream_results
    from swh.core.utils import grouper
    from swh.scheduler.utils import utcnow

    from .utils import get_loader_task_type, send_to_celery

    nb_first_visits = 0
    for lister in backend.get_listers(with_first_visits_to_schedule=True):
        visit_types = backend.get_visit_types_for_listed_origins(lister)
        visit_type_to_queue = {}

        for visit_type in visit_types:
            task_type = get_loader_task_type(backend, visit_type)
            if not task_type:
                raise ValueError(f"Unknown task type for visit type {visit_type}.")
            visit_type_to_queue[visit_type] = (
                f"{lister.first_visits_queue_prefix}:{task_type.backend_name}"
            )

        nb_first_visits += send_to_celery(
            backend,
            visit_type_to_queue=visit_type_to_queue,
            policy="first_visits_after_listing",
            lister_name=lister.name,
            lister_instance_name=lister.instance_name,
        )

        def all_first_visits_scheduled():
            nb_listed_origins = 0
            nb_scheduled_origins = 0
            for listed_origins in grouper(
                stream_results(
                    partial(backend.get_listed_origins, lister_id=lister.id)
                ),
                n=1000,
            ):
                listed_origins_list = list(listed_origins)
                nb_listed_origins += len(listed_origins_list)
                for origin_visit_stats in backend.origin_visit_stats_get(
                    ids=(
                        (origin.url, origin.visit_type)
                        for origin in listed_origins_list
                    )
                ):
                    nb_scheduled_origins += 1
                    if (
                        origin_visit_stats.last_scheduled
                        < lister.last_listing_finished_at
                    ):
                        return False
            return nb_scheduled_origins == nb_listed_origins

        if all_first_visits_scheduled():
            # mark that all first visits were scheduled to no longer consider that
            # lister in future execution of that command
            logger.info(
                "All first visits of origins registered by lister with name '%s' "
                "and instance '%s' were scheduled.'",
                lister.name,
                lister.instance_name,
            )
            lister.first_visits_scheduled_at = utcnow()
            backend.update_lister(lister)

    return nb_first_visits
