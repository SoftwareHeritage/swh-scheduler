.. _swh-scheduler:

.. include:: README.rst

Description
-----------

This module provides two independent scheduler services for the Software
Heritage platform.

The first one is a generic asynchronous task management system allowing to
define tasks with a small number of scheduling properties. In this
documentation, we will call these swh-tasks to prevent confusion. These
swh-tasks are stored in the scheduler database, and a HTTP-based RPC service is
provided to create or find existing swh-task declarations, or select swh-tasks
ready for immediate scheduling.

The second one is specific to the scheduling of origin visits (i.e. loading
tasks). These visits used to be managed by the generic task system presented
above, but this later proved to be less and less suitable to handle the billions
of recurring tasks the origin visits require.

The execution model for all the tasks (generic swh-tasks as well as origin
visits) is using Celery. Thus, each swh-task type defined in the database must
have a (series of) celery worker capable of executing such a swh-task, as well
as there must exist celery workers for origin visits.

As mentioned above, in addition to the original and generic task management
system, ``swh-scheduler`` is now also responsible for keeping track of forge
listing and origin loading statistics. These statistics are used to organize
the scheduling of future loading tasks according to configurable scheduling
policies.

For each of these 2 scheduling management systems, several services are
provided to orchestrate the scheduling of these swh-tasks as Celery tasks on
the one hand, and origin visits as Celery tasks on the other hand.

Generic task scheduler
  The generic task scheduler consists in a database and its access API, along
  with a couple of services.

  First, the ``scheduler-runner`` service is a daemon that regularly looks for
  swh-tasks in the database that should be scheduled. For each of the selected
  swh-task, a Celery task is instantiated.

  There is also a ``scheduler-runner-priority`` service running; this
  is a ``scheduler-runner`` dedicated to schedule tasks with high priority (e.g.
  tasks resulting from the ``save code now`` feature).

  Second, the ``scheduler-listener`` service is a daemon that listen to the
  Celery event bus to maintain scheduled swh-tasks workflow status in the
  database.

Origin visits scheduler
  The scheduler system dedicated to origin visits also consists in a database (it
  is actually the same database as the generic task scheduler one, but using
  dedicated tables) and its access API.

  The ``scheduler-schedule-recurrent`` service is a daemon for choosing which
  origins are to be visited according to scheduling policies and visit
  statistics. It serves the same purpose as the ``scheduler-runner`` from the
  generic task scheduler, but it uses different data model and scheduling
  algorithms.

  Last, there is a ``scheduler-journal-client`` service which listen to the Kafka
  journal of the Storage to maintain the loading tasks status and statistics.
  Once again, the purpose is roughly similar to the ``scheduler-listener`` from
  the generic task scheduler, using Kafka instead of the Celery bus as feedback
  loop.


Generic SWH Task Model
----------------------

Each swh-task-type is the declaration of a type of swh-task. Each swh-task-type
have the following fields:

- ``type``: Name of the swh-task type; can be anything but must be unique,
- ``description``: Human-readable task description
- ``backend_name``: Name of the task in the job-running backend,
- ``default_interval``: Default interval for newly scheduled tasks,
- ``min_interval``: Minimum interval between two runs of a task,
- ``max_interval``: Maximum interval between two runs of a task,
- ``backoff_factor``: Adjustment factor for the backoff between two task runs,
- ``max_queue_length``: Maximum length of the queue for this type of tasks,
- ``num_retries``: Default number of retries on transient failures,
- ``retry_delay``: Retry delay for the task,

Existing swh-task-types can be listed using the ``swh scheduler`` command line
tool::

  $ swh scheduler task-type list
  Known task types:
  check-deposit:
    Pre-checking deposit step before loading into swh archive
  index-fossology-license:
    Fossology license indexer task
  load-git:
    Update an origin of type git
  load-hg:
    Update an origin of type mercurial

You can see the details of a swh-task-type::

  $ swh scheduler task-type list -v -t load-git
  Known task types:
  load-git: swh.loader.git.tasks.UpdateGitRepository
    Update an origin of type git
    interval: 64 days, 0:00:00 [12:00:00, 64 days, 0:00:00]
    backoff_factor: 2.0
    max_queue_length: 5000
    num_retries: None
    retry_delay: None


An swh-task is an 'instance' of such a swh-task-type, and consists in:

- ``arguments``: Arguments passed to the underlying job scheduler,
- ``next_run``: Next run of this task should be run on or after that time,
- ``current_interval``: Interval between two runs of this task, taking into
                      account the backoff factor,
- ``policy``: Whether the task is "one-shot" or "recurring",
- ``retries_left``: Number of "short delay" retries of the task in case of
                  transient failure,
- ``priority``: Priority of the task,
- ``id``: Internal task identifier,
- ``type``: References task_type table,
- ``status``: Task status ( among "next_run_not_scheduled", "next_run_scheduled",
            "completed", "disabled").

So a swh-task consist basically in:

- a set of parameters defining how the scheduling of the
  swh-task is handled,
- a set of parameters to specify the retry policy in case of transient failure
  upon execution,
- a set of parameters that defines the job to be done (``bakend_name`` +
  ``arguments``).


You can list pending swh-tasks (tasks that are to be scheduled ASAP)::

  $ swh scheduler task list-pending load-git --limit 2
  Found 1 load-git tasks

  Task 9052257
    Next run: 15 days ago (2019-06-25 10:35:10+00:00)
    Interval: 2 days, 0:00:00
    Type: load-git
    Policy: recurring
    Args:
      'https://github.com/turtl/mobile'
    Keyword args:


Looking for existing swh-task can be done via the command line tool::

  $ swh scheduler task list -t load-hg --limit 2
  Found 2 tasks

  Task 168802702
    Next run: in 4 hours (2019-07-10 17:56:48+00:00)
    Interval: 1 day, 0:00:00
    Type: load-hg
    Policy: recurring
    Status: next_run_not_scheduled
    Priority:
    Args:
      'https://bitbucket.org/kepung/pypy'
    Keyword args:

  Task 169800445
    Next run: in a month (2019-08-10 17:54:24+00:00)
    Interval: 32 days, 0:00:00
    Type: load-hg
    Policy: recurring
    Status: next_run_not_scheduled
    Priority:
    Args:
      'https://bitbucket.org/lunixbochs/pypy-1'
    Keyword args:


Visit Scheduler
---------------

The visit of an Origin consists in running one of the existing a :ref:`loader
<swh-loader-core>` on the given Origin URL. The visit type will determine which
loader is executed (e.g. the `git` visit type will select the :ref:`git loader
<swh-loader-git>` to perform the loading task).

The part of the scheduler dedicated to origin visits
consists in a few tables in the database to store the knowledge about passed
visits, and a series of tools that handle the scheduling of visit celery tasks
and the updating of the tables.

There are 5 tables related to origin visit scheduling (columns with a `*` are
the primary keys):

- `listers`: Lister instances known to the origin visit scheduler
  - `*id`: UUID,
  - `name`:  Name of the lister (e.g. github, gitlab, debian, ...)
  - `instance_name`: Name of the current instance of this lister (e.g. framagit, bitbucket, ...)
  - `created`: Timestamp at which the lister was originally created
  - `current_state`: Known current state of this lister
  - `updated`: Timestamp at which the lister state was last updated
  - `last_listing_finished_at`: Timestamp at which the last execution of the lister finished
  - `first_visits_queue_prefix`: Optional prefix of message queue names to schedule first visits with high priority
  - `first_visits_scheduled_at`: Timestamp at which all first visits of listed origins have been scheduled with high priority

- `listed_origins`: Origins known to the origin visit scheduler:

  - `*lister_id`: Lister instance which owns this origin (non-null foreign key)
  - `*url`: URL of the origin listed
  - `*visit_type`: Type of the visit which should be scheduled for the given url
  - `extra_loader_arguments`: Extra arguments that should be passed to the
    loader for this origin
  - `enabled`: Whether this origin has been seen during the last listing, and
    visits should be scheduled
  - `first_seen`: Time at which the origin was first seen by a lister
  - `last_seen`: Time at which the origin was last seen by the lister
  - `last_update`: Time of the last update to the origin recorded by the remote
  - `is_fork`: Whether the origin is identified as a fork, if available
  - `forked_from_url`: URL of the origin this origin is forked from, if
    available

- `origin_visit_stats`: Aggregated information on visits for each origin in the
  archive

  - `*url`: Origin URL
  - `*visit_type`: Type of the visit for the given url
  - `last_successful`: Date of the last successful visit, at which we recorded
    the `last_snapshot`
  - `last_visit`: Date of the last visit overall
  - `last_visit_status`: Status of the last visit
  - `last_scheduled`: Time when this origin was scheduled to be visited last
  - `last_snapshot`: sha1_git of the last visit snapshot
  - `next_visit_queue_position`: Position in the global per origin-type queue
    at which some new objects are expected to be found
  - `next_position_offset`: Duration that we expect to wait between visits of
    this origin
  - `successive_visits`: number of successive visits with the same status

- `visit_scheduler_queue_position`: Stores the current queue position for the
  recurrent visit scheduler for each visit type (only used by the
  "origins_without_last_update" scheduling policy)

  - `*visit_type`: Type of the visit for the given url
  - `position`: Current position for the runner of this visit type

- `scheduler_metrics`: Cache of per-lister metrics for the scheduler, collated
  between the listed_origins and origin_visit_stats tables; this table is only
  used to produce the coverage view by the web frontend.

  - `*lister_id`: Lister instance on which metrics have been aggregated
  - `*visit_type`: Visit type on which metrics have been aggregated
  - `last_update`: Last update of these metrics
  - `origins_known`: Number of known (enabled or disabled) origins
  - `origins_enabled`: Number of origins that were present in the latest
    listing
  - `origins_never_visited`: Number of origins that have never been
    successfully visited
  - `origins_with_pending_changes`: Number of enabled origins with known
    activity since our last visit



Scheduling policies
~~~~~~~~~~~~~~~~~~~

Origin visit tasks are scheduled by the `swh scheduler schedule-recurrent`
command line tool. It runs continuously to select, for each supported visit
type, the origins to visit next for the given visit type. It will introspect
the Celery queues to retrieve the number of "free" slots, i.e. the number of
loading tasks of the given type to schedule.

For each visit type, next tasks are selected according to configured policies.
We can apply multiple policies per visit type, each with a given `weight` that
represent the proportion of origins selected according this policy. The
scheduler then relies on previous visits' statistics to select next tasks.

Example
~~~~~~~

Here is a (partial) configuration file for the `swh scheduler
schedule-recurrent` command that declares 3 scheduling policies for scheduling
loading of git repositories:

- 80% of **listed** origins that have never been visited,

- 15% of already visited **listed** origins but with big lag,

- 5% of **listed** origins for which the lister does no provide freshness
  information (sorted by position in the dedicated queue).

.. code-block::

   $ cat scheduler.yml

   [...]

   scheduling_policy:
     git:
       - policy: origins_without_last_update
         weight: 5
       - policy: never_visited_oldest_update_first
         weight: 80
       - policy: already_visited_order_by_lag
         weight: 15

   $ swh scheduler -C scheduler.yml schedule-recurrent


Supported policies are:


- `oldest_scheduled_first`: origins that have never been scheduled for a visit
  (from `origin_visit_stats`)
- `stop_after_success`: origins which last visit has not been eventful (from
  `origin_visit_stats`) sorted by `listed_origins.last_update` (null last) and
  `listed_origins.first_seen`; this is generally used for one-shot visits.
- `never_visited_oldest_update_first`: origins which last snapshot is null
  (i.e. never had an eventful visit), `last_update` is not null (i.e. the
  lister reported some activity for this origin) and sorted by increasing
  `listed_origins.last_update` (i.e. oldest first)
- `already_visited_order_by_lag`: origins that have a `last_snapshot` (thus
  already visited) ignoring origins that have been visited after the known last
  update (from `listed_origins`) order by decreasing lag
- `origins_without_last_update`: origins which `last_update` is null, sorted by
  queue position (never visited first), and unknown origins in order we-ve seen
  then (`listed_origins.first_seen`); this will update the queue position of
  selected origins in the table `origin_visit_stats`
- `first_visits_after_listing`: for a given lister, origin that have never been
  scheduled yet or which last scheduling is older than the last listing, sorted
  by last scheduled (null first).

In addition to these scheduling policies, there are a few parameters that will
impact which origins are selected for scheduling by a given scheduling policy
configuration:

- `absolute_cooldown`: the minimal interval between two visits of the same origin
- `scheduled_cooldown`: the minimal interval before which we can schedule the
  same origin again if it's not been visited
- `failed_cooldown`: the minimal interval before which we can reschedule a
  failed origin
- `not_found_cooldown`: the minimal interval before which we can reschedule a
  not_found origin

.. note::
   - Entries in the `origin_visit_stats` table are added or updated by the `swh
     scheduler journal-client` service which listen to the Kafka journal topic
     `origin_visit_status`
   - Entries in the `listed_origins` table are created or updated by listers.
   - The visit lag is computed, for a given origin resulting from a listing, as
     `listed_origins.last_update - origin_visit_stats.last_successful`.


Writing a new worker for a new swh-task-type
--------------------------------------------

When you want to add a new swh-task-type, you need a celery worker backend
capable of executing this new task-type instances.

Celery workers for swh-scheduler based tasks should be started using the Celery
app in ``swh.scheduler.celery_config``. This later, among other things, provides
a loading mechanism for task types based on ``importlib.metadata``` declared plugins under
the ``[swh.workers]`` entry point.

TODO: add a fully working example of a dumb task.


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli
   simulator

.. only:: standalone_package_doc

   Indices and tables
   ------------------

   * :ref:`genindex`
   * :ref:`modindex`
   * :ref:`search`
