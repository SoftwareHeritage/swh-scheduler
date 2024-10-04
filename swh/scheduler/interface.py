# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


import datetime
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID

from typing_extensions import Protocol, runtime_checkable

from swh.core.api import remote_api_endpoint
from swh.core.api.classes import PagedResult
from swh.scheduler.model import (
    ListedOrigin,
    Lister,
    OriginVisitStats,
    SchedulerMetrics,
    Task,
    TaskPolicy,
    TaskPriority,
    TaskRun,
    TaskRunStatus,
    TaskStatus,
    TaskType,
)

ListedOriginPageToken = Tuple[str, str]


class PaginatedListedOriginList(PagedResult[ListedOrigin, ListedOriginPageToken]):
    """A list of listed origins, with a continuation token"""

    def __init__(
        self,
        results: List[ListedOrigin],
        next_page_token: Union[None, ListedOriginPageToken, List[str]],
    ):
        parsed_next_page_token: Optional[Tuple[str, str]] = None
        if next_page_token is not None:
            if len(next_page_token) != 2:
                raise TypeError("Expected Tuple[str, str] or list of size 2.")
            parsed_next_page_token = tuple(next_page_token)  # type: ignore
        super().__init__(results, parsed_next_page_token)


@runtime_checkable
class SchedulerInterface(Protocol):
    @remote_api_endpoint("task_type/create")
    def create_task_type(self, task_type: TaskType) -> None:
        """Create a new task type in database ready for scheduling.

        Args:
            task_type: a TaskType object
        """
        ...

    @remote_api_endpoint("task_type/get")
    def get_task_type(self, task_type_name: str) -> Optional[TaskType]:
        """Retrieve the registered task type with a given name

        Args:
            task_type_name: name of the task type to retrieve

        Returns:
            a TaskType object or :const:`None` if no such task type exists
        """
        ...

    @remote_api_endpoint("task_type/get_all")
    def get_task_types(self) -> List[TaskType]:
        """Retrieve all registered task types

        Returns:
            a list of TaskType objects
        """
        ...

    @remote_api_endpoint("task/create")
    def create_tasks(
        self, tasks: List[Task], policy: TaskPolicy = "recurring"
    ) -> List[Task]:
        """Register new tasks in database.

        Args:
            tasks: each task is a Task object created with at least the following parameters:

                - type
                - arguments
                - next_run

            policy: default task policy (either recurring or oneshot) to use if not
                set in input task objects


        Returns:
            a list of created tasks with database ids filled.

        """
        ...

    @remote_api_endpoint("task/set_status")
    def set_status_tasks(
        self,
        task_ids: List[int],
        status: TaskStatus = "disabled",
        next_run: Optional[datetime.datetime] = None,
    ) -> None:
        """Set the tasks' status whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers
            status: the status to set for the tasks
            next_run: if provided, also set the next_run date

        """
        ...

    @remote_api_endpoint("task/disable")
    def disable_tasks(self, task_ids: List[int]) -> None:
        """Disable the tasks whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers
        """
        ...

    @remote_api_endpoint("task/search")
    def search_tasks(
        self,
        task_id: Optional[int] = None,
        task_type: Optional[str] = None,
        status: Optional[TaskStatus] = None,
        priority: Optional[TaskPriority] = None,
        policy: Optional[TaskPolicy] = None,
        before: Optional[datetime.datetime] = None,
        after: Optional[datetime.datetime] = None,
        limit: Optional[int] = None,
    ) -> List[Task]:
        """Search tasks from selected criterions

        Args:
            task_id: search a task with given identifier
            task_type: search tasks with given type
            status: search tasks with given status
            priority: search tasks with given priority
            policy: search tasks with given policy
            before: search tasks created before given date
            after: search tasks created after given date
            limit: maximum number of tasks to return

        Returns:
            a list of found tasksa
        """
        ...

    @remote_api_endpoint("task/get")
    def get_tasks(self, task_ids: List[int]) -> List[Task]:
        """Retrieve the info of tasks whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers

        Returns:
            a list of tasks
        """
        ...

    @remote_api_endpoint("task/peek_ready")
    def peek_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        """Fetch the list of tasks (with no priority) to be scheduled.

        Args:
            task_type: filtering task per their type
            timestamp: peek tasks that need to be executed
                before that timestamp
            num_tasks: only peek at num_tasks tasks (with no priority)

        Returns:
            the list of tasks which would be scheduled

        """
        ...

    @remote_api_endpoint("task/grab_ready")
    def grab_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        """Fetch and schedule the list of tasks (with no priority) ready to be scheduled.

        Args:
            task_type: filtering task per their type
            timestamp: grab tasks that need to be executed
                before that timestamp
            num_tasks: only grab num_tasks tasks (with no priority)

        Returns:
            the list of scheduled tasks

        """
        ...

    @remote_api_endpoint("task/peek_ready_with_priority")
    def peek_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        """Fetch list of tasks (with any priority) ready to be scheduled.

        Args:
            task_type: filtering task per their type
            timestamp: peek tasks that need to be executed before that timestamp
            num_tasks: only peek at num_tasks tasks (with no priority)

        Returns:
            the list of tasks which would be scheduled

        """
        ...

    @remote_api_endpoint("task/grab_ready_with_priority")
    def grab_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
    ) -> List[Task]:
        """Fetch and schedule the list of tasks (with any priority) ready to be scheduled.

        Args:
            task_type: filtering task per their type
            timestamp: grab tasks that need to be executed
                before that timestamp
            num_tasks: only grab num_tasks tasks (with no priority)

        Returns:
            the list of scheduled tasks

        """
        ...

    @remote_api_endpoint("task_run/schedule_one")
    def schedule_task_run(
        self,
        task_id: int,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> TaskRun:
        """Mark a given task as scheduled, adding a task_run entry in the database.

        Args:
            task_id: the identifier for the task being scheduled
            backend_id: the identifier of the job in the backend
            metadata: metadata to add to the task_run entry
            timestamp: the instant the event occurred

        Returns:
            a TaskRun object

        """
        ...

    @remote_api_endpoint("task_run/schedule")
    def mass_schedule_task_runs(self, task_runs: List[TaskRun]) -> None:
        """Schedule a bunch of task runs.

        Args:
            task_runs: a list of TaskRun objects created at least with the following parameters:

                - task
                - backend_id
                - scheduled

        """
        ...

    @remote_api_endpoint("task_run/start")
    def start_task_run(
        self,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> Optional[TaskRun]:
        """Mark a given task as started, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id: the identifier of the job in the backend
            metadata: metadata to add to the task_run entry
            timestamp: the instant the event occurred

        Returns:
            a TaskRun object with updated fields

        """
        ...

    @remote_api_endpoint("task_run/end")
    def end_task_run(
        self,
        backend_id: str,
        status: TaskRunStatus,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
    ) -> Optional[TaskRun]:
        """Mark a given task as ended, updating the corresponding task_run entry in the
        database.

        Args:
            backend_id: the identifier of the job in the backend
            status: how the task ended
            metadata: metadata to add to the task_run entry
            timestamp: the instant the event occurred

        Returns:
            a TaskRun object with updated fields

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
    def get_task_runs(
        self, task_ids: List[int], limit: Optional[int] = None
    ) -> List[TaskRun]:
        """Search task run for a task id"""
        ...

    @remote_api_endpoint("listers/get")
    def get_listers(self, with_first_visits_to_schedule: bool = False) -> List[Lister]:
        """Retrieve information about all listers from the database.

        Args:
            with_first_visits_to_schedule: if :const:`True` only retrieve listers whose
                first visits with high priority of listed origins were not scheduled yet
                (those type of listers have the first_visits_queue_prefix attribute set).

        """
        ...

    @remote_api_endpoint("listers/get_by_id")
    def get_listers_by_id(self, lister_ids: List[str]) -> List[Lister]:
        """Retrieve listers in batch, using their UUID"""

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
        self,
        name: str,
        instance_name: Optional[str] = None,
        first_visits_queue_prefix: Optional[str] = None,
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
        urls: Optional[List[str]] = None,
        enabled: Optional[bool] = True,
        limit: int = 1000,
        page_token: Optional[ListedOriginPageToken] = None,
    ) -> PaginatedListedOriginList:
        """Get information on listed origins, possibly filtered, in a paginated way.

        Args:
            lister_id: if provided, return origins discovered with that lister
            url: (deprecated, use ``urls`` parameter instead)
                if provided, return origins matching that URL
            urls: if provided, return origins matching these URLs
            enabled: If :const:`True` return only enabled origins, if :const:`False`
                return only disabled origins, if :const:`None` return all origins.
            limit: maximum number of origins per page
            page_token: to get the next page of origins, is returned in the
                :class:`PaginatedListedOriginList` object

        Returns:
            A page of listed origins
        """
        ...

    @remote_api_endpoint("lister/visit_types")
    def get_visit_types_for_listed_origins(self, lister: Lister) -> List[str]:
        """Return list of distinct visit types for the origins listed by a given lister."""
        ...

    @remote_api_endpoint("origins/grab_next")
    def grab_next_visits(
        self,
        visit_type: str,
        count: int,
        policy: str,
        enabled: bool = True,
        lister_uuid: Optional[str] = None,
        lister_name: Optional[str] = None,
        lister_instance_name: Optional[str] = None,
        timestamp: Optional[datetime.datetime] = None,
        absolute_cooldown: Optional[datetime.timedelta] = datetime.timedelta(hours=12),
        scheduled_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=7),
        failed_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=14),
        not_found_cooldown: Optional[datetime.timedelta] = datetime.timedelta(days=31),
        tablesample: Optional[float] = None,
    ) -> List[ListedOrigin]:
        """Get at most the `count` next origins that need to be visited with
        the `visit_type` loader according to the given scheduling `policy`.

        This will mark the origins as scheduled in the origin_visit_stats
        table, to avoid scheduling multiple visits to the same origin.

        Arguments:
          visit_type: type of visits to schedule
          count: number of visits to schedule
          policy: the scheduling policy used to select which visits to schedule
          enabled: Determine whether we want to list enabled or disabled origins. As
            default, we want reasonably enabled origins. For some edge case, we might
            want the others.
          lister_uuid: Determine the list of origins listed from the lister with uuid
          lister_name: Determine the list of origins listed from the lister with name
          lister_instance_name: Determine the list of origins listed from the lister
            with instance name
          timestamp: the mocked timestamp at which we're recording that the visits are
            being scheduled (defaults to the current time)
          absolute_cooldown: the minimal interval between two visits of the same origin
          scheduled_cooldown: the minimal interval before which we can schedule
            the same origin again if it's not been visited
          failed_cooldown: the minimal interval before which we can reschedule a
            failed origin
          not_found_cooldown: the minimal interval before which we can reschedule a
            not_found origin
          tablesample: the percentage of the table on which we run the query
            (None: no sampling)

        """
        ...

    @remote_api_endpoint("visit_stats/upsert")
    def origin_visit_stats_upsert(
        self, origin_visit_stats: Iterable[OriginVisitStats]
    ) -> None:
        """Create a new origin visit stats"""
        ...

    @remote_api_endpoint("visit_stats/get")
    def origin_visit_stats_get(
        self, ids: Iterable[Tuple[str, str]]
    ) -> List[OriginVisitStats]:
        """Retrieve the visit statistics for an origin with a given visit type.

        .. warning::

            If some visit statistics are not found, they are filtered out of the result.
            So the output list may be shorter than the input list.

        Args:
            ids: an iterable of (origin_url, visit_type) tuples

        Returns:
            a list of origin visit statistics

        """
        ...

    @remote_api_endpoint("visit_scheduler/get")
    def visit_scheduler_queue_position_get(
        self,
    ) -> Dict[str, int]:
        """Retrieve all current queue positions for the recurrent visit scheduler.

        Returns
            Mapping of visit type to their current queue position

        """
        ...

    @remote_api_endpoint("visit_scheduler/set")
    def visit_scheduler_queue_position_set(
        self, visit_type: str, position: int
    ) -> None:
        """Set the current queue position of the recurrent visit scheduler for `visit_type`."""
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
