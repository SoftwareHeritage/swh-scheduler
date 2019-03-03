create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

comment on table dbversion is 'Schema update tracking';

insert into dbversion (version, release, description)
       values (12, now(), 'Work In Progress');

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

