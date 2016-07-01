-- SWH Scheduler Schema upgrade
-- from_version: 02
-- to_version: 03
-- description: Allow mass-creation of one-shot task

begin;

insert into dbversion (version, release, description)
values (3, now(), 'Work In Progress');

create table oneshot_task_type (
  type text primary key,
  description text not null,
  backend_name text not null
);

comment on table oneshot_task_type is 'Types of one-shot tasks';
comment on column oneshot_task_type.type is 'Short identifier for the task type, e.g. git-updater, svn-loader, etc...';
comment on column oneshot_task_type.description is 'Human-readable task description';
comment on column oneshot_task_type.backend_name is 'Name of the task in the job-running backend, e.g. Git repository updater, Svn repository loader, etc...';

create index on oneshot_task_type(type);

create table oneshot_task (
  id bigserial primary key,
  type text not null references oneshot_task_type(type),
  arguments jsonb not null,
  next_run timestamptz not null default now()
);

comment on table oneshot_task is 'One shot tasks to be scheduled as soon as possible.';
comment on column oneshot_task.id is 'Unique identifier on that task';
comment on column oneshot_task.type is 'Short identifier for the task type, e.g. git-updater, svn-loader, etc...';
comment on column oneshot_task.arguments is 'Arguments passed to the underlying job scheduler. '
'Contains two keys, ''args'' (list) and ''kwargs'' (object).';
comment on column oneshot_task.next_run is 'The next run of this task should be run on or after that time. Default to 60 seconds after insertion.';

insert into oneshot_task_type(type, description, backend_name)
values('svn-loader', 'Svn Repository Loader', 'swh.loader.svn.tasks.LoadSvnRepositoryTsk');

commit;
