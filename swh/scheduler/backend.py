# Copyright (C) 2015-2024  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import json
import logging
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple, Union
from uuid import UUID

import attr
from psycopg2.errors import CardinalityViolation
from psycopg2.extensions import AsIs
import psycopg2.extras
import psycopg2.pool
from testing.postgresql import Postgresql

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction
from swh.core.db.db_utils import init_admin_extensions, populate_database_for_package
from swh.scheduler.model import (
    Task,
    TaskArguments,
    TaskPolicy,
    TaskPriority,
    TaskRun,
    TaskRunStatus,
    TaskStatus,
    TaskType,
)
from swh.scheduler.utils import utcnow

from .exc import SchedulerException, StaleData, UnknownPolicy
from .interface import ListedOriginPageToken, PaginatedListedOriginList
from .model import (
    LastVisitStatus,
    ListedOrigin,
    Lister,
    OriginVisitStats,
    SchedulerMetrics,
)

logger = logging.getLogger(__name__)


def adapt_LastVisitStatus(v: LastVisitStatus):
    return AsIs(f"'{v.value}'::last_visit_status")


psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)
psycopg2.extensions.register_adapter(LastVisitStatus, adapt_LastVisitStatus)
psycopg2.extras.register_uuid()


def format_query(query: str, keys: Sequence[str]) -> str:
    """Format a query with the given keys"""

    query_keys = ", ".join(keys)
    placeholders = ", ".join(["%s"] * len(keys))

    return query.format(keys=query_keys, placeholders=placeholders)


def mutate_task_dict(task_dict: Dict[str, Any]) -> Dict[str, Any]:
    task_dict["arguments"] = TaskArguments(**task_dict["arguments"])
    return task_dict


class SchedulerBackend:
    """Backend for the Software Heritage scheduling database."""

    current_version = 38

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

    task_type_keys = [field.name for field in attr.fields(TaskType)]

    @db_transaction()
    def create_task_type(self, task_type: TaskType, db=None, cur=None) -> None:
        """Create a new task type ready for scheduling.

        Args:
            task_type: a TaskType dictionary
        """
        query = format_query(
            """insert into task_type ({keys}) values ({placeholders})
            on conflict do nothing""",
            self.task_type_keys,
        )
        cur.execute(query, task_type.to_tuple())

    @db_transaction()
    def get_task_type(
        self, task_type_name: str, db=None, cur=None
    ) -> Optional[TaskType]:
        """Retrieve the task type with id task_type_name"""
        query = format_query(
            "select {keys} from task_type where type=%s",
            self.task_type_keys,
        )
        cur.execute(query, (task_type_name,))
        row = cur.fetchone()
        return TaskType(**row) if row is not None else None

    @db_transaction()
    def get_task_types(self, db=None, cur=None) -> List[TaskType]:
        """Retrieve all registered task types"""
        query = format_query(
            "select {keys} from task_type",
            self.task_type_keys,
        )
        cur.execute(query)
        return [TaskType(**row) for row in cur.fetchall()]

    @db_transaction()
    def get_listers(
        self, with_first_visits_to_schedule: bool = False, db=None, cur=None
    ) -> List[Lister]:
        """Retrieve information about all listers from the database."""

        select_cols = ", ".join(Lister.select_columns())

        query = f"""
            select {select_cols} from listers
        """
        if with_first_visits_to_schedule:
            query += """where last_listing_finished_at is not null
                        and first_visits_queue_prefix is not null
                        and first_visits_scheduled_at is null"""

        cur.execute(query)

        return [Lister(**ret) for ret in cur.fetchall()]

    @db_transaction()
    def get_listers_by_id(
        self, lister_ids: List[str], db=None, cur=None
    ) -> List[Lister]:
        """Retrieve listers in batch, using their UUID"""
        select_cols = ", ".join(Lister.select_columns())

        query = f"""
            select {select_cols} from listers
              where id in %s
        """

        if not lister_ids:
            return []

        cur.execute(query, (tuple(lister_ids),))

        return [Lister(**row) for row in cur]

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
        self,
        name: str,
        instance_name: Optional[str] = None,
        first_visits_queue_prefix: Optional[str] = None,
        db=None,
        cur=None,
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

        cur.execute(
            query,
            Lister(
                name=name,
                instance_name=instance_name,
                first_visits_queue_prefix=first_visits_queue_prefix,
            ).to_dict(),
        )

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

        cur.execute(query, lister.to_dict())
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

        deduplicated_origins = {
            tuple(getattr(origin, k) for k in pk_cols): origin
            for origin in listed_origins
        }

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
            argslist=(origin.to_dict() for origin in deduplicated_origins.values()),
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
        urls: Optional[List[str]] = None,
        enabled: Optional[bool] = True,
        limit: int = 1000,
        page_token: Optional[ListedOriginPageToken] = None,
        db=None,
        cur=None,
    ) -> PaginatedListedOriginList:
        """Get information on the listed origins matching either the `url` or
        `lister_id`, or both arguments.
        """

        query_filters: List[str] = []
        query_params: List[Union[int, str, UUID, Tuple[UUID, str], Tuple[str, ...]]] = (
            []
        )

        if lister_id:
            query_filters.append("lister_id = %s")
            query_params.append(lister_id)

        urls_ = []
        if url is not None:
            urls_.append(url)
        elif urls:
            urls_ = urls

        if urls_:
            query_filters.append("url IN %s")
            query_params.append(tuple(urls_))

        if enabled is not None:
            query_filters.append("enabled = %s")
            query_params.append(enabled)

        if page_token is not None:
            query_filters.append("(lister_id, url) > %s")
            # the typeshed annotation for tuple() is too strict.
            query_params.append(tuple(page_token))

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
    def get_visit_types_for_listed_origins(
        self, lister: Lister, db=None, cur=None
    ) -> List[str]:
        query = """select distinct(visit_type) from listed_origins
                    where lister_id = %s"""
        cur.execute(query, (lister.id,))
        return [row["visit_type"] for row in cur]

    @db_transaction()
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
        db=None,
        cur=None,
    ) -> List[ListedOrigin]:
        if timestamp is None:
            timestamp = utcnow()

        origin_select_cols = ", ".join(ListedOrigin.select_columns())

        joins: Dict[str, str] = {
            "origin_visit_stats": "USING (url, visit_type)",
        }

        query_args: List[Any] = []

        where_clauses = []

        # list of (name, query) handled as CTEs before the main query
        common_table_expressions: List[Tuple[str, str]] = []

        # "NOT enabled" = the lister said the origin no longer exists
        where_clauses.append("enabled" if enabled else "not enabled")

        # Only schedule visits of the given type
        where_clauses.append("visit_type = %s")
        query_args.append(visit_type)

        if absolute_cooldown:
            # Don't schedule visits if they've been scheduled since the absolute cooldown
            where_clauses.append(
                """origin_visit_stats.last_scheduled IS NULL
                   OR origin_visit_stats.last_scheduled < %s
                """
            )
            query_args.append(timestamp - absolute_cooldown)

        if scheduled_cooldown:
            # Don't re-schedule visits if they're already scheduled but we haven't
            # recorded a result yet, unless they've been scheduled more than a week
            # ago (it probably means we've lost them in flight somewhere).
            where_clauses.append(
                """origin_visit_stats.last_scheduled IS NULL
                OR origin_visit_stats.last_scheduled < GREATEST(
                  %s,
                  origin_visit_stats.last_visit
                )
            """
            )
            query_args.append(timestamp - scheduled_cooldown)

        if failed_cooldown:
            # Don't retry failed origins too often
            where_clauses.append(
                "origin_visit_stats.last_visit_status is distinct from 'failed' "
                "or origin_visit_stats.last_visit < %s"
            )
            query_args.append(timestamp - failed_cooldown)

        if not_found_cooldown:
            # Don't retry not found origins too often
            where_clauses.append(
                "origin_visit_stats.last_visit_status is distinct from 'not_found' "
                "or origin_visit_stats.last_visit < %s"
            )
            query_args.append(timestamp - not_found_cooldown)

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
                "listed_origins.last_update > origin_visit_stats.last_successful"
            )

            # order by decreasing visit lag
            order_by = (
                "listed_origins.last_update - origin_visit_stats.last_successful DESC"
            )
        elif policy == "origins_without_last_update":
            where_clauses.append("last_update IS NULL")
            order_by = ", ".join(
                [
                    # By default, sort using the queue position. If the queue
                    # position is null, then the origin has never been visited,
                    # which we want to handle first
                    "origin_visit_stats.next_visit_queue_position nulls first",
                    # Schedule unknown origins in the order we've seen them
                    "listed_origins.first_seen",
                ]
            )

            # fmt: off

            # This policy requires updating the global queue position for this
            # visit type
            common_table_expressions.append(("update_queue_position", """
                INSERT INTO
                  visit_scheduler_queue_position(visit_type, position)
                SELECT
                  visit_type, COALESCE(MAX(next_visit_queue_position), 0)
                FROM selected_origins
                GROUP BY visit_type
                ON CONFLICT(visit_type) DO UPDATE
                  SET position=GREATEST(
                    visit_scheduler_queue_position.position, EXCLUDED.position
                  )
            """))
            # fmt: on
        elif policy == "first_visits_after_listing":
            assert lister_uuid is not None or (
                lister_name is not None and lister_instance_name is not None
            ), "first_visits_after_listing policy requires lister info "
            if lister_uuid is not None:
                listers = self.get_listers_by_id([lister_uuid])
                lister = listers[0] if listers else None
            else:
                lister = self.get_lister(lister_name, lister_instance_name)
            assert (
                lister is not None
            ), f"Lister with name {lister_name} and instance {lister_instance_name} not found !"
            where_clause = "origin_visit_stats.last_scheduled IS NULL"
            if lister.last_listing_finished_at:
                where_clause += " OR origin_visit_stats.last_scheduled < %s"
                query_args.append(lister.last_listing_finished_at)
            where_clauses.append(where_clause)
            order_by = "origin_visit_stats.last_scheduled NULLS FIRST"
        else:
            raise UnknownPolicy(f"Unknown scheduling policy {policy}")

        if tablesample:
            table = "listed_origins tablesample SYSTEM (%s)"
            query_args.insert(0, tablesample)
        else:
            table = "listed_origins"

        if lister_uuid:
            where_clauses.append("lister_id = %s")
            query_args.append(lister_uuid)

        if lister_name:
            joins["listers"] = "on listed_origins.lister_id=listers.id"
            where_clauses.append("listers.name = %s")
            query_args.append(lister_name)

        if lister_instance_name:
            joins["listers"] = "on listed_origins.lister_id=listers.id"
            where_clauses.append("listers.instance_name = %s")
            query_args.append(lister_instance_name)

        join_clause = "\n".join(
            f"left join {table} {clause}" for table, clause in joins.items()
        )

        # fmt: off
        common_table_expressions.insert(0, ("selected_origins", f"""
            SELECT
              {origin_select_cols}, next_visit_queue_position
            FROM
              {table}
            {join_clause}
            WHERE
              ({") AND (".join(where_clauses)})
            ORDER BY
              {order_by}
            LIMIT %s
        """))
        # fmt: on

        query_args.append(count)

        # fmt: off
        common_table_expressions.append(("deduplicated_selected_origins", """
          SELECT DISTINCT
            url, visit_type
          FROM
            selected_origins
        """))
        # fmt: on

        # fmt: off
        common_table_expressions.append(("update_stats", """
            INSERT INTO
              origin_visit_stats (url, visit_type, last_scheduled)
            SELECT
              url, visit_type, %s
            FROM
              deduplicated_selected_origins
            ON CONFLICT (url, visit_type) DO UPDATE
              SET last_scheduled = GREATEST(
                origin_visit_stats.last_scheduled,
                EXCLUDED.last_scheduled
              )
        """))
        # fmt: on

        query_args.append(timestamp)

        formatted_ctes = ",\n".join(
            f"{name} AS (\n{cte}\n)" for name, cte in common_table_expressions
        )

        query = f"""
            WITH
              {formatted_ctes}
            SELECT
              {origin_select_cols}
            FROM
              selected_origins
        """

        cur.execute(query, tuple(query_args))
        return [ListedOrigin(**d) for d in cur]

    task_keys = [field.name for field in attr.fields(Task)]
    task_create_keys = [
        key for key in task_keys if key not in {"id", "current_interval"}
    ]

    @db_transaction()
    def create_tasks(
        self, tasks: List[Task], policy: TaskPolicy = "recurring", db=None, cur=None
    ) -> List[Task]:
        """Create new tasks.

        Args:
            tasks: each task is a Task object created with at least the following parameters:

                - type
                - arguments
                - next_run

            policy: default task policy (either recurring or oneshot) to use if not
                set in input task objects

        Returns:
            a list of created tasks with database ids filled

        """
        cur.execute("select swh_scheduler_mktemp_task()")
        db.copy_to(
            (task.to_dict() for task in tasks),
            "tmp_task",
            self.task_create_keys,
            default_values={"policy": policy},
            cur=cur,
        )
        query = format_query(
            "select {keys} from swh_scheduler_create_tasks_from_temp()",
            self.task_keys,
        )
        cur.execute(query)
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def set_status_tasks(
        self,
        task_ids: List[int],
        status: TaskStatus = "disabled",
        next_run: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
    ) -> None:
        """Set the tasks' status whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers
            status: the status to set for the tasks
            next_run: if provided, also set the next_run date

        """
        if not task_ids:
            return
        query = ["UPDATE task SET status = %s"]
        args: List[Any] = [status]
        if next_run:
            query.append(", next_run = %s")
            args.append(next_run)
        query.append(" WHERE id IN %s")
        args.append(tuple(task_ids))

        cur.execute("".join(query), args)

    @db_transaction()
    def disable_tasks(self, task_ids: List[int], db=None, cur=None) -> None:
        """Disable the tasks whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers
        """
        self.set_status_tasks(task_ids, db=db, cur=cur)

    @db_transaction()
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
        db=None,
        cur=None,
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
            a list of found tasks
        """
        where = []
        args: List[Any] = []

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
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def get_tasks(self, task_ids: List[int], db=None, cur=None) -> List[Task]:
        """Retrieve the info of tasks whose ids are listed.

        Args:
            task_ids: list of tasks' identifiers

        Returns:
            a list of Task objects
        """
        query = format_query("select {keys} from task where id in %s", self.task_keys)
        cur.execute(query, (tuple(task_ids),))
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def peek_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
        db=None,
        cur=None,
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
        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            """select * from swh_scheduler_peek_no_priority_tasks(
                %s, %s, %s :: bigint)""",
            (task_type, timestamp, num_tasks),
        )
        logger.debug("PEEK %s => %s" % (task_type, cur.rowcount))
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def grab_ready_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
        db=None,
        cur=None,
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
        if timestamp is None:
            timestamp = utcnow()
        cur.execute(
            """select * from swh_scheduler_grab_ready_tasks(
                 %s, %s, %s :: bigint)""",
            (task_type, timestamp, num_tasks),
        )
        logger.debug("GRAB %s => %s" % (task_type, cur.rowcount))
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def peek_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
        db=None,
        cur=None,
    ) -> List[Task]:
        """Fetch list of tasks (with any priority) ready to be scheduled.

        Args:
            task_type: filtering task per their type
            timestamp: peek tasks that need to be executed before that timestamp
            num_tasks: only peek at num_tasks tasks (with no priority)

        Returns:
            the list of tasks which would be scheduled

        """
        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            """select * from swh_scheduler_peek_any_ready_priority_tasks(
                %s, %s, %s :: bigint)""",
            (task_type, timestamp, num_tasks),
        )
        logger.debug("PEEK %s => %s", task_type, cur.rowcount)
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    @db_transaction()
    def grab_ready_priority_tasks(
        self,
        task_type: str,
        timestamp: Optional[datetime.datetime] = None,
        num_tasks: Optional[int] = None,
        db=None,
        cur=None,
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
        if timestamp is None:
            timestamp = utcnow()
        cur.execute(
            """select * from swh_scheduler_grab_any_ready_priority_tasks(
                 %s, %s, %s :: bigint)""",
            (task_type, timestamp, num_tasks),
        )
        logger.debug("GRAB %s => %s", task_type, cur.rowcount)
        return [Task(**mutate_task_dict(row)) for row in cur.fetchall()]

    task_run_create_keys = ["task", "backend_id", "scheduled", "metadata"]

    @db_transaction()
    def schedule_task_run(
        self,
        task_id: int,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
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

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_schedule_task_run(%s, %s, %s, %s)",
            (task_id, backend_id, metadata, timestamp),
        )

        row = cur.fetchone()
        return TaskRun(**row)

    @db_transaction()
    def mass_schedule_task_runs(
        self, task_runs: List[TaskRun], db=None, cur=None
    ) -> None:
        """Schedule a bunch of task runs.

        Args:
            task_runs: a list of TaskRun objects created at least with the following parameters:

                - task
                - backend_id
                - scheduled
        """
        cur.execute("select swh_scheduler_mktemp_task_run()")
        db.copy_to(
            (task_run.to_dict() for task_run in task_runs),
            "tmp_task_run",
            self.task_run_create_keys,
            cur=cur,
        )
        cur.execute("select swh_scheduler_schedule_task_run_from_temp()")

    @db_transaction()
    def start_task_run(
        self,
        backend_id: str,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
    ) -> Optional[TaskRun]:
        """Mark a given task as started, updating the corresponding task_run
           entry in the database.

        Args:
            backend_id: the identifier of the job in the backend
            metadata: metadata to add to the task_run entry
            timestamp: the instant the event occurred

        Returns:
            a TaskRun object with updated fields, or None if there was no
            TaskRun recorded with a matching backend_id.

        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_start_task_run(%s, %s, %s)",
            (backend_id, metadata, timestamp),
        )

        for row in cur:
            if row["status"] is not None:
                return TaskRun(**row)

        logger.debug(
            "Failed to mark task run %s as started",
            backend_id,
        )
        return None

    @db_transaction()
    def end_task_run(
        self,
        backend_id: str,
        status: TaskRunStatus,
        metadata: Optional[Dict[str, Any]] = None,
        timestamp: Optional[datetime.datetime] = None,
        db=None,
        cur=None,
    ) -> Optional[TaskRun]:
        """Mark a given task as ended, updating the corresponding task_run entry in the
        database.

        Args:
            backend_id: the identifier of the job in the backend
            status: how the task ended
            metadata: metadata to add to the task_run entry
            timestamp: the instant the event occurred

        Returns:
            a TaskRun object with updated fields, or None if there was no
            TaskRun recorded with a matching backend_id.

        """

        if metadata is None:
            metadata = {}

        if timestamp is None:
            timestamp = utcnow()

        cur.execute(
            "select * from swh_scheduler_end_task_run(%s, %s, %s, %s)",
            (backend_id, status, metadata, timestamp),
        )

        for row in cur:
            if row["status"] is not None:
                return TaskRun(**row)

        logger.debug(
            "Failed to mark task run %s as ended",
            backend_id,
        )
        return None

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
    def get_task_runs(
        self, task_ids: List[int], limit: Optional[int] = None, db=None, cur=None
    ) -> List[TaskRun]:
        """Search task run for a task id"""
        if task_ids:
            args: List[Any] = [tuple(task_ids)]
            query = "select * from task_run where task in %s"
            if limit:
                query += " limit %s :: bigint"
                args.append(limit)
            cur.execute(query, args)
            return [TaskRun(**row) for row in cur.fetchall()]
        else:
            return []

    @db_transaction()
    def origin_visit_stats_upsert(
        self, origin_visit_stats: Iterable[OriginVisitStats], db=None, cur=None
    ) -> None:
        pk_cols = OriginVisitStats.primary_key_columns()
        insert_cols, insert_meta = OriginVisitStats.insert_columns_and_metavars()

        upsert_cols = [col for col in insert_cols if col not in pk_cols]
        upsert_set = ", ".join(
            f"{col} = coalesce(EXCLUDED.{col}, ovi.{col})" for col in upsert_cols
        )

        query = f"""
            INSERT into origin_visit_stats AS ovi ({", ".join(insert_cols)})
            VALUES %s
            ON CONFLICT ({", ".join(pk_cols)}) DO UPDATE
            SET {upsert_set}
        """

        try:
            psycopg2.extras.execute_values(
                cur=cur,
                sql=query,
                argslist=(visit_stats.to_dict() for visit_stats in origin_visit_stats),
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
        rows = psycopg2.extras.execute_values(
            cur=cur, sql=query, argslist=primary_keys, fetch=True
        )
        return [OriginVisitStats(**row) for row in rows]

    @db_transaction()
    def visit_scheduler_queue_position_get(self, db=None, cur=None) -> Dict[str, int]:
        cur.execute("SELECT visit_type, position FROM visit_scheduler_queue_position")
        return {row["visit_type"]: row["position"] for row in cur}

    @db_transaction()
    def visit_scheduler_queue_position_set(
        self,
        visit_type: str,
        position: int,
        db=None,
        cur=None,
    ) -> None:
        query = """
            INSERT INTO visit_scheduler_queue_position(visit_type, position)
            VALUES(%s, %s)
            ON CONFLICT(visit_type) DO UPDATE SET position=EXCLUDED.position
        """
        cur.execute(query, (visit_type, position))

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


class TemporarySchedulerBackend(SchedulerBackend):
    """Temporary postgresql backend for the Software Heritage scheduling database.

    A temporary scheduler database is spawned then removed when the backend
    gets destroyed.

    It can be used for testing SWH components that require a scheduler instance
    (listers for instance).
    """

    def __init__(self):
        self.postgresql = Postgresql()
        init_admin_extensions("swh.scheduler", self.postgresql.url())
        populate_database_for_package("swh.scheduler", self.postgresql.url())
        super().__init__(self.postgresql.url())
