create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

comment on table dbversion is 'Schema update tracking';
comment on column dbversion.version is 'SQL schema version';
comment on column dbversion.release is 'Version deployment timestamp';
comment on column dbversion.description is 'Version description';

insert into dbversion (version, release, description)
       values (17, now(), 'Work In Progress');

create table task_type (
  type text primary key,
  description text not null,
  backend_name text not null,
  default_interval interval,
  min_interval interval,
  max_interval interval,
  backoff_factor float,
  max_queue_length bigint,
  num_retries bigint,
  retry_delay interval
);

comment on table task_type is 'Types of schedulable tasks';
comment on column task_type.type is 'Short identifier for the task type';
comment on column task_type.description is 'Human-readable task description';
comment on column task_type.backend_name is 'Name of the task in the job-running backend';
comment on column task_type.default_interval is 'Default interval for newly scheduled tasks';
comment on column task_type.min_interval is 'Minimum interval between two runs of a task';
comment on column task_type.max_interval is 'Maximum interval between two runs of a task';
comment on column task_type.backoff_factor is 'Adjustment factor for the backoff between two task runs';
comment on column task_type.max_queue_length is 'Maximum length of the queue for this type of tasks';
comment on column task_type.num_retries is 'Default number of retries on transient failures';
comment on column task_type.retry_delay is 'Retry delay for the task';

create type task_status as enum ('next_run_not_scheduled', 'next_run_scheduled', 'completed', 'disabled');
comment on type task_status is 'Status of a given task';

create type task_policy as enum ('recurring', 'oneshot');
comment on type task_policy is 'Recurrence policy of the given task';

create type task_priority as enum('high', 'normal', 'low');
comment on type task_priority is 'Priority of the given task';

create table priority_ratio(
  id task_priority primary key,
  ratio float not null
);

comment on table priority_ratio is 'Oneshot task''s reading ratio per priority';
comment on column priority_ratio.id is 'Task priority id';
comment on column priority_ratio.ratio is 'Percentage of tasks to read per priority';

insert into priority_ratio (id, ratio) values ('high', 0.5);
insert into priority_ratio (id, ratio) values ('normal', 0.3);
insert into priority_ratio (id, ratio) values ('low', 0.2);

create table task (
  id bigserial primary key,
  type text not null references task_type(type),
  arguments jsonb not null,
  next_run timestamptz not null,
  current_interval interval,
  status task_status not null,
  policy task_policy not null default 'recurring',
  retries_left bigint not null default 0,
  priority task_priority references priority_ratio(id),
  check (policy <> 'recurring' or current_interval is not null)
);

comment on table task is 'Schedule of recurring tasks';
comment on column task.arguments is 'Arguments passed to the underlying job scheduler. '
                                    'Contains two keys, ''args'' (list) and ''kwargs'' (object).';
comment on column task.next_run is 'The next run of this task should be run on or after that time';
comment on column task.current_interval is 'The interval between two runs of this task, '
                                           'taking into account the backoff factor';
comment on column task.policy is 'Whether the task is one-shot or recurring';
comment on column task.retries_left is 'The number of "short delay" retries of the task in case of '
                                       'transient failure';
comment on column task.priority is 'Policy of the given task';
comment on column task.id is 'Task Identifier';
comment on column task.type is 'References task_type table';
comment on column task.status is 'Task status (''next_run_not_scheduled'', ''next_run_scheduled'', ''completed'', ''disabled'')';

create type task_run_status as enum ('scheduled', 'started', 'eventful', 'uneventful', 'failed', 'permfailed', 'lost');
comment on type task_run_status is 'Status of a given task run';

create table task_run (
  id bigserial primary key,
  task bigint not null references task(id),
  backend_id text,
  scheduled timestamptz,
  started timestamptz,
  ended timestamptz,
  metadata jsonb,
  status task_run_status not null default 'scheduled'
);
comment on table task_run is 'History of task runs sent to the job-running backend';
comment on column task_run.backend_id is 'id of the task run in the job-running backend';
comment on column task_run.metadata is 'Useful metadata for the given task run. '
                                       'For instance, the worker that took on the job, '
                                       'or the logs for the run.';
comment on column task_run.id is 'Task run identifier';
comment on column task_run.task is 'References task table';
comment on column task_run.scheduled is 'Scheduled run time for task';
comment on column task_run.started is 'Task starting time';
comment on column task_run.ended is 'Task ending time';

create table if not exists listers (
  id uuid primary key default uuid_generate_v4(),
  name text not null,
  instance_name text not null,
  created timestamptz not null default now(),  -- auto_now_add in the model
  current_state jsonb not null,
  updated timestamptz not null
);

comment on table listers is 'Lister instances known to the origin visit scheduler';
comment on column listers.name is 'Name of the lister (e.g. github, gitlab, debian, ...)';
comment on column listers.instance_name is 'Name of the current instance of this lister (e.g. framagit, bitbucket, ...)';
comment on column listers.created is 'Timestamp at which the lister was originally created';
comment on column listers.current_state is 'Known current state of this lister';
comment on column listers.updated is 'Timestamp at which the lister state was last updated';


create table if not exists listed_origins (
  -- Basic information
  lister_id uuid not null references listers(id),
  url text not null,
  visit_type text not null,
  extra_loader_arguments jsonb not null,

  -- Whether this origin still exists or not
  enabled boolean not null,

  -- time-based information
  first_seen timestamptz not null default now(),
  last_seen timestamptz not null,

  -- potentially provided by the lister
  last_update timestamptz,

  primary key (lister_id, url, visit_type)
);

comment on table listed_origins is 'Origins known to the origin visit scheduler';
comment on column listed_origins.lister_id is 'Lister instance which owns this origin';
comment on column listed_origins.url is 'URL of the origin listed';
comment on column listed_origins.visit_type is 'Type of the visit which should be scheduled for the given url';
comment on column listed_origins.extra_loader_arguments is 'Extra arguments that should be passed to the loader for this origin';

comment on column listed_origins.enabled is 'Whether this origin has been seen during the last listing, and visits should be scheduled.';
comment on column listed_origins.first_seen is 'Time at which the origin was first seen by a lister';
comment on column listed_origins.last_seen is 'Time at which the origin was last seen by the lister';

comment on column listed_origins.last_update is 'Time of the last update to the origin recorded by the remote';
