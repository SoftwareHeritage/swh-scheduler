.. _swh-scheduler:

Software Heritage - Job scheduler
=================================

Task manager for asynchronous/delayed tasks, used for both recurrent (e.g.,
listing a forge, loading new stuff from a Git repository) and one-off
activities (e.g., loading a specific version of a source package).


Description
-----------

This module provides a scheduler service for the Software Heritage platform. It
allows to define tasks with a number of properties. In this documentation, we
will call these swh-tasks to prevent confusion. These swh-tasks are stored in
a database, and a HTTP-based RPC service is provided to create or find existing
swh-task declarations.

The execution model for these swh-tasks is using Celery. Thus, each swh-task
type defined in the database must have a (series of) celery worker capable of
executing such a swh-task.

Then a number of services are also provided to manage the scheduling of these
swh-tasks as Celery tasks.

The `scheduler-runner` service is a daemon that regularly looks for swh-tasks
in the database that should be scheduled. For each of the selected swh-task, a
Celery task is instantiated.

The `scheduler-listener` service is a daemon that listen to the Celery event
bus and maintain scheduled swh-tasks workflow status.


SWH Task Model
~~~~~~~~~~~~~~

Each swh-task-type is the declaration of a type of swh-task. Each swh-task-type
have the following fields:

- `type`: Name of the swh-task type; can be anything but must be unique,
- `description`: Human-readable task description
- `backend_name`: Name of the task in the job-running backend,
- `default_interval`: Default interval for newly scheduled tasks,
- `min_interval`: Minimum interval between two runs of a task,
- `max_interval`: Maximum interval between two runs of a task,
- `backoff_factor`: Adjustment factor for the backoff between two task runs,
- `max_queue_length`: Maximum length of the queue for this type of tasks,
- `num_retries`: Default number of retries on transient failures,
- `retry_delay`: Retry delay for the task,

Existing swh-task-types can be listed using the `swh scheduler` command line
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

- `arguments`: Arguments passed to the underlying job scheduler,
- `next_run`: Next run of this task should be run on or after that time,
- `current_interval`: Interval between two runs of this task, taking into
                      account the backoff factor,
- `policy`: Whether the task is "one-shot" or "recurring",
- `retries_left`: Number of "short delay" retries of the task in case of
                  transient failure,
- `priority`: Priority of the task,
- `id`: Internal task identifier,
- `type`: References task_type table,
- `status`: Task status ( among "next_run_not_scheduled", "next_run_scheduled",
            "completed", "disabled").

So a swh-task consist basically in:

- a set of parameters defining how the scheduling of the
  swh-task is handled,
- a set of parameters to specify the retry policy in case of transient failure
  upon execution,
- a set of parameters that defines the job to be done (`bakend_name` +
  `arguments`).


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



Writing a new worker for a new swh-task-type
--------------------------------------------

When you want to add a new swh-task-type, you need a celery worker backend
capable of executing this new task-type instances.

Celery workers for swh-scheduler based tasks should be started using the Celery
app in `swh.scheduler.celery_config`. This later, among other things, provides
a loading mechanism for task types based on pkg_resources declared plugins under
the `[swh.workers]` entry point.

TODO: add a fully working example of a dumb task.


Reference Documentation
-----------------------

.. toctree::
   :maxdepth: 2

   cli
   simulator
   /apidoc/swh.scheduler
