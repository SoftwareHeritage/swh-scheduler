# Copyright (C) 2015-2021  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Tuple, Union
from uuid import UUID

import attr
from psycopg2.errors import CardinalityViolation
import psycopg2.extras
import psycopg2.pool

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction
from swh.scheduler.utils import utcnow

from .exc import SchedulerException, StaleData, UnknownPolicy
from .interface import ListedOriginPageToken, PaginatedListedOriginList
from .model import ListedOrigin, Lister, OriginVisitStats, SchedulerMetrics

logger = logging.getLogger(__name__)


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extras.register_uuid()


def format_query(query, keys):
    """Format a query with the given keys"""

    query_keys = ", ".join(keys)
    placeholders = ", ".join(["%s"] * len(keys))

    return query.format(keys=query_keys, placeholders=placeholders)


class SchedulerBackend:
    """Backend for the Software Heritage scheduling database.

    """

    def __init__(self, db, min_pool_conns=1, max_pool_conns=10):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
        if isinstance(db, psycopg2.extensions.connection):
            self._pool = None
            self._db = BaseDb(db)
        else:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                min_pool_conns,
                max_pool_conns,
                db,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            self._db = None

    def get_db(self):
        if self._db:
            return self._db
        return BaseDb.from_pool(self._pool)

    def put_db(self, db):
        if db is not self._db:
            db.put_conn()

    task_type_keys = [
        "type",
        "description",
        "backend_name",
        "default_interval",
        "min_interval",
        "max_interval",
        "backoff_factor",
        "max_queue_length",
        "num_retries",
        "retry_delay",
    ]

    @db_transaction()
    def create_task_type(self, task_type, db=None, cur=None):
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
        keys = [key for key in self.task_type_keys if key in task_type]
        query = format_query(
            """insert into task_type ({keys}) values ({placeholders})
            on conflict do nothing""",
            keys,
        )
        cur.execute(query, [task_type[key] for key in keys])

    @db_transaction()
    def get_task_type(self, task_type_name, db=None, cur=None):
        """Retrieve the task type with id task_type_name"""
        query = format_query(
            "select {keys} from task_type where type=%s", self.task_type_keys,
        )
        cur.execute(query, (task_type_name,))
        return cur.fetchone()

    @db_transaction()
    def get_task_types(self, db=None, cur=None):
        """Retrieve all registered task types"""
        query = format_query("select {keys} from task_type", self.task_type_keys,)
        cur.execute(query)
        return cur.fetchall()

    @db_transaction()
    def get_lister(
        self, name: str, instance_name: Optional[str] = None, db=None, cur=None
    ) -> Optional[Lister]:
        """Retrieve information about the given instance of the lister from the
        database.
        """
        if instance_name is None:
            instance_name = ""

        select_cols = ", ".join(Lister.select_columns())

        query = f"""
            select {select_cols} from listers
              where (name, instance_name) = (%s, %s)
        """

        cur.execute(query, (name, instance_name))

        ret = cur.fetchone()
        if not ret:
            return None

        return Lister(**ret)

    @db_transaction()
    def get_or_create_lister(
        self, name: str, instance_name: Optional[str] = None, db=None, cur=None
    ) -> Lister:
        """Retrieve information about the given instance of the lister from the
        database, or create the entry if it did not exist.
        """

        if instance_name is None:
            instance_name = ""

        select_cols = ", ".join(Lister.select_columns())
        insert_cols, insert_meta = (
            ", ".join(tup) for tup in Lister.insert_columns_and_metavars()
        )

        query = f"""
            with added as (
              insert into listers ({insert_cols}) values ({insert_meta})
                on conflict do nothing
                returning {select_cols}
            )
            select {select_cols} from added
          union all
            select {select_cols} from listers
              where (name, instance_name) = (%(name)s, %(instance_name)s);
        """

        cur.execute(query, attr.asdict(Lister(name=name, instance_name=instance_name)))

        return Lister(**cur.fetchone())

    @db_transaction()
    def update_lister(self, lister: Lister, db=None, cur=None) -> Lister:
        """Update the state for the given lister instance in the database.

        Returns:
            a new Lister object, with all fields updated from the database

        Raises:
            StaleData if the `updated` timestamp for the lister instance in
        database doesn't match the one passed by the user.
        """

        select_cols = ", ".join(Lister.select_columns())
        set_vars = ", ".join(
            f"{col} = {meta}"
            for col, meta in zip(*Lister.insert_columns_and_metavars())
        )

        query = f"""update listers
                      set {set_vars}
                      where id=%(id)s and updated=%(updated)s
                      returning {select_cols}"""

        cur.execute(query, attr.asdict(lister))
        updated = cur.fetchone()

        if not updated:
            raise StaleData("Stale data; Lister state not updated")

        return Lister(**updated)

    @db_transaction()
    def record_listed_origins(
        self, listed_origins: Iterable[ListedOrigin], db=None, cur=None
    ) -> List[ListedOrigin]:
        """Record a set of origins that a lister has listed.

        This performs an "upsert": origins with the same (lister_id, url,
        visit_type) values are updated with new values for
        extra_loader_arguments, last_update and last_seen.
        """

        pk_cols = ListedOrigin.primary_key_columns()
        select_cols = ListedOrigin.select_columns()
        insert_cols, insert_meta = ListedOrigin.insert_columns_and_metavars()

        upsert_cols = [col for col in insert_cols if col not in pk_cols]
        upsert_set = ", ".join(f"{col} = EXCLUDED.{col}" for col in upsert_cols)

        query = f"""INSERT into listed_origins ({", ".join(insert_cols)})
                       VALUES %s
                    ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE
                       SET {upsert_set}
                    RETURNING {", ".join(select_cols)}
        """

        ret = psycopg2.extras.execute_values(
            cur=cur,
            sql=query,
            argslist=(attr.asdict(origin) for origin in listed_origins),
            template=f"({', '.join(insert_meta)})",
            page_size=1000,
            fetch=True,
        )

        return [ListedOrigin(**d) for d in ret]

    @db_transaction()
    def get_listed_origins(
        self,
        lister_id: Optional[UUID] = None,
        url: Optional[str] = None,
        limit: int = 1000,
        page_token: Optional[ListedOriginPageToken] = None,
        db=None,
        cur=None,
    ) -> PaginatedListedOriginList:
        """Get information on the listed origins matching either the `url` or
        `lister_id`, or both arguments.
        """

        query_filters: List[str] = []
        query_params: List[Union[int, str, UUID, Tuple[UUID, str]]] = []

        if lister_id:
            query_filters.append("lister_id = %s")
            query_params.append(lister_id)

        if url is not None:
            query_filters.append("url = %s")
            query_params.append(url)

        if page_token is not None:
            query_filters.append("(lister_id, url) > %s")
            # the typeshed annotation for tuple() is too strict.
            query_params.append(tuple(page_token))  # type: ignore

        query_params.append(limit)

        select_cols = ", ".join(ListedOrigin.select_columns())
        if query_filters:
            where_clause = "where %s" % (" and ".join(query_filters))
        else:
            where_clause = ""

        query = f"""SELECT {select_cols}
                    from listed_origins
                    {where_clause}
                    ORDER BY lister_id, url
                    LIMIT %s"""

        cur.execute(query, tuple(query_params))
        origins = [ListedOrigin(**d) for d in cur]

        if len(origins) == limit:
            page_token = (str(origins[-1].lister_id), origins[-1].url)
        else:
            page_token = None

        return PaginatedListedOriginList(origins, page_token)

    @db_transaction()
    def grab_next_visits(
        self,
        visit_type: str,
        count: int,
        policy: str,
        timestamp: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
    ) -> List[ListedOrigin]:
        """Get at most the `count` next origins that need to be visited with
        the `visit_type` loader according to the given scheduling `policy`.

        This will mark the origins as scheduled in the origin_visit_stats
        table, to avoid scheduling multiple visits to the same origin.

        Arguments:
          visit_type: type of visits to schedule
          count: number of visits to schedule
          policy: the scheduling policy used to select which visits to schedule
          timestamp: the mocked timestamp at which we're recording that the visits are
            being scheduled (defaults to the current time)
        """
        if timestamp is None:
            timestamp = utcnow()

        origin_select_cols = ", ".join(ListedOrigin.select_columns())

        query_args: List[Any] = []

        where_clauses = []

        # "NOT enabled" = the lister said the origin no longer exists
        where_clauses.append("enabled")

        # Only schedule visits of the given type
        where_clauses.append("visit_type = %s")
        query_args.append(visit_type)

        # Don't re-schedule visits if they're already scheduled but we haven't
        # recorded a result yet, unless they've been scheduled more than a week
        # ago (it probably means we've lost them in flight somewhere).
        where_clauses.append(
            """origin_visit_stats.last_scheduled IS NULL
            OR origin_visit_stats.last_scheduled < GREATEST(
              %s - '7 day'::interval,
              origin_visit_stats.last_eventful,
              origin_visit_stats.last_uneventful,
              origin_visit_stats.last_failed,
              origin_visit_stats.last_notfound
            )
        """
        )
        query_args.append(timestamp)

        if policy == "oldest_scheduled_first":
            order_by = "origin_visit_stats.last_scheduled NULLS FIRST"
        elif policy == "never_visited_oldest_update_first":
            # never visited origins have a NULL last_snapshot
            where_clauses.append("origin_visit_stats.last_snapshot IS NULL")

            # order by increasing last_update (oldest first)
            where_clauses.append("listed_origins.last_update IS NOT NULL")
            order_by = "listed_origins.last_update"
        elif policy == "already_visited_order_by_lag":
            # TODO: store "visit lag" in a materialized view?

            # visited origins have a NOT NULL last_snapshot
            where_clauses.append("origin_visit_stats.last_snapshot IS NOT NULL")

            # ignore origins we have visited after the known last update
            where_clauses.append("listed_origins.last_update IS NOT NULL")
            where_clauses.append(
                """
              listed_origins.last_update
              > GREATEST(
                origin_visit_stats.last_eventful,
                origin_visit_stats.last_uneventful
              )
            """
            )

            # order by decreasing visit lag
            order_by = """\
              listed_origins.last_update
              - GREATEST(
                origin_visit_stats.last_eventful,
                origin_visit_stats.last_uneventful
              )
              DESC
            """
        else:
            raise UnknownPolicy(f"Unknown scheduling policy {policy}")

        select_query = f"""
          SELECT
            {origin_select_cols}
          FROM
            listed_origins
          LEFT JOIN
            origin_visit_stats USING (url, visit_type)
          WHERE
            ({") AND (".join(where_clauses)})
          ORDER BY
            {order_by}
          LIMIT %s
        """
        query_args.append(count)

        query = f"""
            WITH selected_origins AS (
               {select_query}
            ),
            update_stats AS (
              INSERT INTO
                origin_visit_stats (
                  url, visit_type, last_scheduled
                )
              SELECT
                url, visit_type, %s
              FROM
                selected_origins
              ON CONFLICT (url, visit_type) DO UPDATE
                SET last_scheduled = GREATEST(
                  origin_visit_stats.last_scheduled,
                  EXCLUDED.last_scheduled
                )
            )
            SELECT
              *
            FROM
              selected_origins
        """
        query_args.append(timestamp)

        cur.execute(query, tuple(query_args))
        return [ListedOrigin(**d) for d in cur]

    task_create_keys = [
        "type",
        "arguments",
        "next_run",
        "policy",
        "status",
        "retries_left",
        "priority",
    ]
    task_keys = task_create_keys + ["id", "current_interval"]

    @db_transaction()
    def create_tasks(self, tasks, policy="recurring", db=None, cur=None):
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
        cur.execute("select swh_scheduler_mktemp_task()")
        db.copy_to(
            tasks,
            "tmp_task",
            self.task_create_keys,
            default_values={"policy": policy, "status": "next_run_not_scheduled"},
            cur=cur,
        )
        query = format_query(
            "select {keys} from swh_scheduler_create_tasks_from_temp()", self.task_keys,
        )
        cur.execute(query)
        return cur.fetchall()

    @db_transaction()
    def set_status_tasks(
        self, task_ids, status="disabled", next_run=None, db=None, cur=None
    ):
        """Set the tasks' status whose ids are listed.

        If given, also set the next_run date.
        """
        if not task_ids:
            return
        query = ["UPDATE task SET status = %s"]
        args = [status]
        if next_run:
            query.append(", next_run = %s")
            args.append(next_run)
        query.append(" WHERE id IN %s")
        args.append(tuple(task_ids))

        cur.execute("".join(query), args)

    @db_transaction()
    def disable_tasks(self, task_ids, db=None, cur=None):
        """Disable the tasks whose ids are listed."""
        return self.set_status_tasks(task_ids, db=db, cur=cur)

    @db_transaction()
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
        db=None,
        cur=None,
    ):
        """Search tasks from selected criterions"""
        where = []
        args = []

        if task_id:
            if isinstance(task_id, (str, int)):
                where.append("id = %s")
            else:
                where.append("id in %s")
                task_id = tuple(task_id)
            args.append(task_id)
        if task_type:
            if isinstance(task_type, str):
                where.append("type = %s")
            else:
                where.append("type in %s")
                task_type = tuple(task_type)
            args.append(task_type)
        if status:
            if isinstance(status, str):
                where.append("status = %s")
            else:
                where.append("status in %s")
                status = tuple(status)
            args.append(status)
        if priority:
            if isinstance(priority, str):
                where.append("priority = %s")
            else:
                priority = tuple(priority)
                where.append("priority in %s")
            args.append(priority)
        if policy:
            where.append("policy = %s")
            args.append(policy)
        if before:
            where.append("next_run <= %s")
            args.append(before)
        if after:
            where.append("next_run >= %s")
            args.append(after)

        query = "select * from task"
        if where:
            query += " where " + " and ".join(where)
        if limit:
            query += " limit %s :: bigint"
            args.append(limit)
        cur.execute(query, args)
        return cur.fetchall()

    @db_transaction()
    def get_tasks(self, task_ids, db=None, cur=None):
        """Retrieve the info of tasks whose ids are listed."""
        query = format_query("select {keys} from task where id in %s", self.task_keys)
        cur.execute(query, (tuple(task_ids),))
        return cur.fetchall()

    @db_transaction()
    def peek_ready_tasks(
        self,
        task_type,
        timestamp=None,
        num_tasks=None,
        num_tasks_priority=None,
        db=None,
        cur=None,
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
        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            """select * from swh_scheduler_peek_ready_tasks(
                %s, %s, %s :: bigint, %s :: bigint)""",
            (task_type, timestamp, num_tasks, num_tasks_priority),
        )
        logger.debug("PEEK %s => %s" % (task_type, cur.rowcount))
        return cur.fetchall()

    @db_transaction()
    def grab_ready_tasks(
        self,
        task_type,
        timestamp=None,
        num_tasks=None,
        num_tasks_priority=None,
        db=None,
        cur=None,
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
        if timestamp is None:
            timestamp = utcnow()
        cur.execute(
            """select * from swh_scheduler_grab_ready_tasks(
                 %s, %s, %s :: bigint, %s :: bigint)""",
            (task_type, timestamp, num_tasks, num_tasks_priority),
        )
        logger.debug("GRAB %s => %s" % (task_type, cur.rowcount))
        return cur.fetchall()

    task_run_create_keys = ["task", "backend_id", "scheduled", "metadata"]

    @db_transaction()
    def schedule_task_run(
        self, task_id, backend_id, metadata=None, timestamp=None, db=None, cur=None
    ):
        """Mark a given task as scheduled, adding a task_run entry in the database.

        Args:
            task_id (int): the identifier for the task being scheduled
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred

        Returns:
            a fresh task_run entry

        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_schedule_task_run(%s, %s, %s, %s)",
            (task_id, backend_id, metadata, timestamp),
        )

        return cur.fetchone()

    @db_transaction()
    def mass_schedule_task_runs(self, task_runs, db=None, cur=None):
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
        cur.execute("select swh_scheduler_mktemp_task_run()")
        db.copy_to(task_runs, "tmp_task_run", self.task_run_create_keys, cur=cur)
        cur.execute("select swh_scheduler_schedule_task_run_from_temp()")

    @db_transaction()
    def start_task_run(
        self, backend_id, metadata=None, timestamp=None, db=None, cur=None
    ):
        """Mark a given task as started, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id (str): the identifier of the job in the backend
            metadata (dict): metadata to add to the task_run entry
            timestamp (datetime.datetime): the instant the event occurred

        Returns:
            the updated task_run entry

        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_start_task_run(%s, %s, %s)",
            (backend_id, metadata, timestamp),
        )

        return cur.fetchone()

    @db_transaction()
    def end_task_run(
        self,
        backend_id,
        status,
        metadata=None,
        timestamp=None,
        result=None,
        db=None,
        cur=None,
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

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_end_task_run(%s, %s, %s, %s)",
            (backend_id, status, metadata, timestamp),
        )
        return cur.fetchone()

    @db_transaction()
    def filter_task_to_archive(
        self,
        after_ts: str,
        before_ts: str,
        limit: int = 10,
        page_token: Optional[str] = None,
        db=None,
        cur=None,
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
        assert not page_token or isinstance(page_token, str)
        last_id = -1 if page_token is None else int(page_token)
        tasks = []
        cur.execute(
            "select * from swh_scheduler_task_to_archive(%s, %s, %s, %s)",
            (after_ts, before_ts, last_id, limit + 1),
        )
        for row in cur:
            task = dict(row)
            # nested type index does not accept bare values
            # transform it as a dict to comply with this
            task["arguments"]["args"] = {
                i: v for i, v in enumerate(task["arguments"]["args"])
            }
            kwargs = task["arguments"]["kwargs"]
            task["arguments"]["kwargs"] = json.dumps(kwargs)
            tasks.append(task)

        if len(tasks) >= limit + 1:  # remains data, add pagination information
            result = {
                "tasks": tasks[:limit],
                "next_page_token": str(tasks[-1]["task_id"]),
            }
        else:
            result = {"tasks": tasks}

        return result

    @db_transaction()
    def delete_archived_tasks(self, task_ids, db=None, cur=None):
        """Delete archived tasks as much as possible. Only the task_ids whose
           complete associated task_run have been cleaned up will be.

        """
        _task_ids = _task_run_ids = []
        for task_id in task_ids:
            _task_ids.append(task_id["task_id"])
            _task_run_ids.append(task_id["task_run_id"])

        cur.execute(
            "select * from swh_scheduler_delete_archived_tasks(%s, %s)",
            (_task_ids, _task_run_ids),
        )

    task_run_keys = [
        "id",
        "task",
        "backend_id",
        "scheduled",
        "started",
        "ended",
        "metadata",
        "status",
    ]

    @db_transaction()
    def get_task_runs(self, task_ids, limit=None, db=None, cur=None):
        """Search task run for a task id"""
        where = []
        args = []

        if task_ids:
            if isinstance(task_ids, (str, int)):
                where.append("task = %s")
            else:
                where.append("task in %s")
                task_ids = tuple(task_ids)
            args.append(task_ids)
        else:
            return ()

        query = "select * from task_run where " + " and ".join(where)
        if limit:
            query += " limit %s :: bigint"
            args.append(limit)
        cur.execute(query, args)
        return cur.fetchall()

    @db_transaction()
    def get_priority_ratios(self, db=None, cur=None):
        cur.execute("select id, ratio from priority_ratio")
        return {row["id"]: row["ratio"] for row in cur.fetchall()}

    @db_transaction()
    def origin_visit_stats_upsert(
        self, origin_visit_stats: Iterable[OriginVisitStats], db=None, cur=None
    ) -> None:
        pk_cols = OriginVisitStats.primary_key_columns()
        insert_cols, insert_meta = OriginVisitStats.insert_columns_and_metavars()

        query = f"""
            INSERT into origin_visit_stats AS ovi ({", ".join(insert_cols)})
            VALUES %s
            ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE
            SET last_eventful = coalesce(excluded.last_eventful, ovi.last_eventful),
                last_uneventful = coalesce(excluded.last_uneventful, ovi.last_uneventful),
                last_failed = coalesce(excluded.last_failed, ovi.last_failed),
                last_notfound = coalesce(excluded.last_notfound, ovi.last_notfound),
                last_snapshot = coalesce(excluded.last_snapshot, ovi.last_snapshot)
        """  # noqa

        try:
            psycopg2.extras.execute_values(
                cur=cur,
                sql=query,
                argslist=(
                    attr.asdict(visit_stats) for visit_stats in origin_visit_stats
                ),
                template=f"({', '.join(insert_meta)})",
                page_size=1000,
                fetch=False,
            )
        except CardinalityViolation as e:
            raise SchedulerException(repr(e))

    @db_transaction()
    def origin_visit_stats_get(
        self, ids: Iterable[Tuple[str, str]], db=None, cur=None
    ) -> List[OriginVisitStats]:
        if not ids:
            return []
        primary_keys = tuple((origin, visit_type) for (origin, visit_type) in ids)
        query = format_query(
            """
            SELECT {keys}
            FROM (VALUES %s) as stats(url, visit_type)
            INNER JOIN origin_visit_stats USING (url, visit_type)
        """,
            OriginVisitStats.select_columns(),
        )
        psycopg2.extras.execute_values(cur=cur, sql=query, argslist=primary_keys)
        return [OriginVisitStats(**row) for row in cur.fetchall()]

    @db_transaction()
    def update_metrics(
        self,
        lister_id: Optional[UUID] = None,
        timestamp: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
    ) -> List[SchedulerMetrics]:
        """Update the performance metrics of this scheduler instance.

        Returns the updated metrics.

        Args:
          lister_id: if passed, update the metrics only for this lister instance
          timestamp: if passed, the date at which we're updating the metrics,
            defaults to the database NOW()
        """
        query = format_query(
            "SELECT {keys} FROM update_metrics(%s, %s)",
            SchedulerMetrics.select_columns(),
        )
        cur.execute(query, (lister_id, timestamp))
        return [SchedulerMetrics(**row) for row in cur.fetchall()]

    @db_transaction()
    def get_metrics(
        self,
        lister_id: Optional[UUID] = None,
        visit_type: Optional[str] = None,
        db=None,
        cur=None,
    ) -> List[SchedulerMetrics]:
        """Retrieve the performance metrics of this scheduler instance.

        Args:
          lister_id: filter the metrics for this lister instance only
          visit_type: filter the metrics for this visit type only
        """

        where_filters = []
        where_args = []
        if lister_id:
            where_filters.append("lister_id = %s")
            where_args.append(str(lister_id))
        if visit_type:
            where_filters.append("visit_type = %s")
            where_args.append(visit_type)

        where_clause = ""
        if where_filters:
            where_clause = f"where {' and '.join(where_filters)}"

        query = format_query(
            "SELECT {keys} FROM scheduler_metrics %s" % where_clause,
            SchedulerMetrics.select_columns(),
        )

        cur.execute(query, tuple(where_args))
        return [SchedulerMetrics(**row) for row in cur.fetchall()]
