# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple
from uuid import UUID

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.scheduler.model import (
    ListedOrigin,
    ListedOriginPageToken,
    Lister,
    OriginVisitStats,
    PaginatedListedOriginList,
    SchedulerMetrics,
)


@runtime_checkable
class SchedulerInterface(Protocol):
    @remote_api_endpoint("task_type/create")
    def create_task_type(self, task_type):
        """Create a new task type ready for scheduling.

        Args:
            task_type (dict): a dictionary with the following keys:

                - type (str): an identifier for the task type
                - description (str): a human-readable description of what the
                  task does
                - backend_name (str): the name of the task in the
                  job-scheduling backend
                - default_interval (datetime.timedelta): the default interval
                  between two task runs
                - min_interval (datetime.timedelta): the minimum interval
                  between two task runs
                - max_interval (datetime.timedelta): the maximum interval
                  between two task runs
                - backoff_factor (float): the factor by which the interval
                  changes at each run
                - max_queue_length (int): the maximum length of the task queue
                  for this task type

        """
        ...

    @remote_api_endpoint("task_type/get")
    def get_task_type(self, task_type_name):
        """Retrieve the task type with id task_type_name"""
        ...

    @remote_api_endpoint("task_type/get_all")
    def get_task_types(self):
        """Retrieve all registered task types"""
        ...

    @remote_api_endpoint("task/create")
    def create_tasks(self, tasks, policy="recurring"):
        """Create new tasks.

        Args:
            tasks (list): each task is a dictionary with the following keys:

                - type (str): the task type
                - arguments (dict): the arguments for the task runner, keys:

                      - args (list of str): arguments
                      - kwargs (dict str -> str): keyword arguments

                - next_run (datetime.datetime): the next scheduled run for the
                  task

        Returns:
            a list of created tasks.

        """
        ...

    @remote_api_endpoint("task/set_status")
    def set_status_tasks(self, task_ids, status="disabled", next_run=None):
        """Set the tasks' status whose ids are listed.

        If given, also set the next_run date.
        """
        ...

    @remote_api_endpoint("task/disable")
    def disable_tasks(self, task_ids):
        """Disable the tasks whose ids are listed."""
        ...

    @remote_api_endpoint("task/search")
    def search_tasks(
        self,
        task_id=None,
        task_type=None,
        status=None,
        priority=None,
        policy=None,
        before=None,
        after=None,
        limit=None,
    ):
        """Search tasks from selected criterions"""
        ...

    @remote_api_endpoint("task/get")
    def get_tasks(self, task_ids):
        """Retrieve the info of tasks whose ids are listed."""
        ...

    @remote_api_endpoint("task/peek_ready")
    def peek_ready_tasks(
        self, task_type, timestamp=None, num_tasks=None, num_tasks_priority=None,
    ):
        """Fetch the list of ready tasks

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): peek tasks that need to be executed
                before that timestamp
            num_tasks (int): only peek at num_tasks tasks (with no priority)
            num_tasks_priority (int): only peek at num_tasks_priority
                                      tasks (with priority)

        Returns:
            a list of tasks

        """
        ...

    @remote_api_endpoint("task/grab_ready")
    def grab_ready_tasks(
        self, task_type, timestamp=None, num_tasks=None, num_tasks_priority=None,
    ):
        """Fetch the list of ready tasks, and mark them as scheduled

        Args:
            task_type (str): filtering task per their type
            timestamp (datetime.datetime): grab tasks that need to be executed
                before that timestamp
            num_tasks (int): only grab num_tasks tasks (with no priority)
            num_tasks_priority (int): only grab oneshot num_tasks tasks (with
                                      priorities)

        Returns:
            a list of tasks

        """
        ...

    @remote_api_endpoint("task_run/schedule_one")
    def schedule_task_run(self, task_id, backend_id, metadata=None, timestamp=None):
        """Mark a given task as scheduled, adding a task_run entry in the database.

        Args:
            task_id (int): the identifier for the task being scheduled
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred

        Returns:
            a fresh task_run entry

        """
        ...

    @remote_api_endpoint("task_run/schedule")
    def mass_schedule_task_runs(self, task_runs):
        """Schedule a bunch of task runs.

        Args:
            task_runs (list): a list of dicts with keys:

                - task (int): the identifier for the task being scheduled
                - backend_id (str): the identifier of the job in the backend
                - metadata (dict): metadata to add to the task_run entry
                - scheduled (datetime.datetime): the instant the event occurred

        Returns:
            None
        """
        ...

    @remote_api_endpoint("task_run/start")
    def start_task_run(self, backend_id, metadata=None, timestamp=None):
        """Mark a given task as started, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred

        Returns:
            the updated task_run entry

        """
        ...

    @remote_api_endpoint("task_run/end")
    def end_task_run(
        self, backend_id, status, metadata=None, timestamp=None, result=None,
    ):
        """Mark a given task as ended, updating the corresponding task_run entry in the
        database.

        Args:
            backend_id (str): the identifier of the job in the backend
            status (str): how the task ended; one of: 'eventful', 'uneventful',
                'failed'
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred

        Returns:
            the updated task_run entry

        """
        ...

    @remote_api_endpoint("task/filter_for_archive")
    def filter_task_to_archive(
        self,
        after_ts: str,
        before_ts: str,
        limit: int = 10,
        page_token: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Compute the tasks to archive within the datetime interval
        [after_ts, before_ts[. The method returns a paginated result.

        Returns:
            dict with the following keys:
              - **next_page_token**: opaque token to be used as
                `page_token` to retrieve the next page of result. If absent,
                there is no more pages to gather.
              - **tasks**: list of task dictionaries with the following keys:

                    **id** (str): origin task id
                    **started** (Optional[datetime]): started date
                    **scheduled** (datetime): scheduled date
                    **arguments** (json dict): task's arguments
                    ...

        """
        ...

    @remote_api_endpoint("task/delete_archived")
    def delete_archived_tasks(self, task_ids):
        """Delete archived tasks as much as possible. Only the task_ids whose
           complete associated task_run have been cleaned up will be.

        """
        ...

    @remote_api_endpoint("task_run/get")
    def get_task_runs(self, task_ids, limit=None):
        """Search task run for a task id"""
        ...

    @remote_api_endpoint("lister/get")
    def get_lister(
        self, name: str, instance_name: Optional[str] = None
    ) -> Optional[Lister]:
        """Retrieve information about the given instance of the lister from the
        database.
        """
        ...

    @remote_api_endpoint("lister/get_or_create")
    def get_or_create_lister(
        self, name: str, instance_name: Optional[str] = None
    ) -> Lister:
        """Retrieve information about the given instance of the lister from the
        database, or create the entry if it did not exist.
        """
        ...

    @remote_api_endpoint("lister/update")
    def update_lister(self, lister: Lister) -> Lister:
        """Update the state for the given lister instance in the database.

        Returns:
            a new Lister object, with all fields updated from the database

        Raises:
            StaleData if the `updated` timestamp for the lister instance in
        database doesn't match the one passed by the user.
        """
        ...

    @remote_api_endpoint("origins/record")
    def record_listed_origins(
        self, listed_origins: Iterable[ListedOrigin]
    ) -> List[ListedOrigin]:
        """Record a set of origins that a lister has listed.

        This performs an "upsert": origins with the same (lister_id, url,
        visit_type) values are updated with new values for
        extra_loader_arguments, last_update and last_seen.
        """
        ...

    @remote_api_endpoint("origins/get")
    def get_listed_origins(
        self,
        lister_id: Optional[UUID] = None,
        url: Optional[str] = None,
        limit: int = 1000,
        page_token: Optional[ListedOriginPageToken] = None,
    ) -> PaginatedListedOriginList:
        """Get information on the listed origins matching either the `url` or
        `lister_id`, or both arguments.

        Use the `limit` and `page_token` arguments for continuation. The next
        page token, if any, is returned in the PaginatedListedOriginList object.
        """
        ...

    @remote_api_endpoint("origins/grab_next")
    def grab_next_visits(
        self, visit_type: str, count: int, policy: str
    ) -> List[ListedOrigin]:
        """Get at most the `count` next origins that need to be visited with
        the `visit_type` loader according to the given scheduling `policy`.

        This will mark the origins as "being visited" in the listed_origins
        table, to avoid scheduling multiple visits to the same origin.
        """
        ...

    @remote_api_endpoint("priority_ratios/get")
    def get_priority_ratios(self):
        ...

    @remote_api_endpoint("visit_stats/upsert")
    def origin_visit_stats_upsert(
        self, origin_visit_stats: Iterable[OriginVisitStats]
    ) -> None:
        """Create a new origin visit stats
        """
        ...

    @remote_api_endpoint("visit_stats/get")
    def origin_visit_stats_get(
        self, ids: Iterable[Tuple[str, str]]
    ) -> List[OriginVisitStats]:
        """Retrieve the stats for an origin with a given visit type

        If some visit_stats are not found, they are filtered out of the result. So the
        output list may be of length inferior to the length of the input list.

        """
        ...

    @remote_api_endpoint("scheduler_metrics/update")
    def update_metrics(
        self,
        lister_id: Optional[UUID] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> List[SchedulerMetrics]:
        """Update the performance metrics of this scheduler instance.

        Returns the updated metrics.

        Args:
          lister_id: if passed, update the metrics only for this lister instance
          timestamp: if passed, the date at which we're updating the metrics,
            defaults to the database NOW()
        """
        ...

    @remote_api_endpoint("scheduler_metrics/get")
    def get_metrics(
        self, lister_id: Optional[UUID] = None, visit_type: Optional[str] = None
    ) -> List[SchedulerMetrics]:
        """Retrieve the performance metrics of this scheduler instance.

        Args:
          lister_id: filter the metrics for this lister instance only
          visit_type: filter the metrics for this visit type only
        """
        ...
