create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

comment on table dbversion is 'Schema update tracking';

insert into dbversion (version, release, description)
       values (3, now(), 'Work In Progress');

create table task_type (
  type text primary key,
  description text not null,
  backend_name text not null,
  default_interval interval not null,
  min_interval interval not null,
  max_interval interval not null,
  backoff_factor float not null
);

comment on table task_type is 'Types of schedulable tasks';
comment on column task_type.type is 'Short identifier for the task type';
comment on column task_type.description is 'Human-readable task description';
comment on column task_type.backend_name is 'Name of the task in the job-running backend';
comment on column task_type.default_interval is 'Default interval for newly scheduled tasks';
comment on column task_type.min_interval is 'Minimum interval between two runs of a task';
comment on column task_type.max_interval is 'Maximum interval between two runs of a task';
comment on column task_type.backoff_factor is 'Adjustment factor for the backoff between two task runs';

create type task_status as enum ('next_run_not_scheduled', 'next_run_scheduled', 'disabled');
comment on type task_status is 'Status of a given task';

create table task (
  id bigserial primary key,
  type text not null references task_type(type),
  arguments jsonb not null,
  next_run timestamptz not null,
  current_interval interval not null,
  status task_status not null
);
comment on table task is 'Schedule of recurring tasks';
comment on column task.arguments is 'Arguments passed to the underlying job scheduler. '
                                    'Contains two keys, ''args'' (list) and ''kwargs'' (object).';
comment on column task.next_run is 'The next run of this task should be run on or after that time';
comment on column task.current_interval is 'The interval between two runs of this task, '
                                           'taking into account the backoff factor';

create index on task(type);
create index on task(next_run);
create index task_args on task using btree ((arguments -> 'args'));
create index task_kwargs on task using gin ((arguments -> 'kwargs'));

create type task_run_status as enum ('scheduled', 'started', 'eventful', 'uneventful', 'failed', 'lost');
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

create index on task_run(task);
create index on task_run(backend_id);

create table oneshot_task_type (
  type text primary key,
  description text not null,
  backend_name text not null
);

create index on oneshot_task_type(type);

comment on table oneshot_task_type is 'Types of one-shot tasks';
comment on column oneshot_task_type.type is 'Short identifier for the task type, e.g. git-updater, svn-loader, etc...';
comment on column oneshot_task_type.description is 'Human-readable task description';
comment on column oneshot_task_type.backend_name is 'Name of the task in the job-running backend, e.g. Git repository updater, Svn repository loader, etc...';

create table oneshot_task (
  id bigserial primary key,
  type text not null references oneshot_task_type(type),
  arguments jsonb not null,
  next_run timestamptz not null default now() + interval '60 s'
);

comment on table oneshot_task is 'One shot tasks to be scheduled as soon as possible.';
comment on column oneshot_task.id is 'Unique identifier on that task';
comment on column oneshot_task.type is 'Short identifier for the task type, e.g. git-updater, svn-loader, etc...';
comment on column oneshot_task.arguments is 'Arguments passed to the underlying job scheduler. '
'Contains two keys, ''args'' (list) and ''kwargs'' (object).';
comment on column oneshot_task.next_run is 'The next run of this task should be run on or after that time. Default to 60 seconds after insertion.';

insert into oneshot_task_type(type, description, backend_name)
values('svn-loader', 'Svn Repository Loader', 'swh.loader.svn.tasks.LoadSvnRepositoryTsk');

create or replace function swh_scheduler_mktemp_task ()
  returns void
  language sql
as $$
  create temporary table tmp_task (
    like task excluding indexes
  ) on commit drop;
  alter table tmp_task
    drop column id,
    drop column current_interval,
    drop column status;
$$;

comment on function swh_scheduler_mktemp_task () is 'Create a temporary table for bulk task creation';


create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, status, current_interval)
    select type, arguments, next_run, 'next_run_not_scheduled',
           (select default_interval from task_type tt where tt.type = type)
      from tmp_task
  returning task.*;
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';

create or replace function swh_scheduler_peek_ready_tasks (ts timestamptz default now(),
                                                           num_tasks bigint default NULL)
  returns setof task
  language sql
  stable
as $$
select * from task
  where next_run <= ts
  and status = 'next_run_not_scheduled'
  order by next_run
  limit num_tasks;
$$;

create or replace function swh_scheduler_grab_ready_tasks (ts timestamptz default now(),
                                                           num_tasks bigint default NULL)
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
      select id from task
        where next_run <= ts
              and status='next_run_not_scheduled'
        order by next_run
        limit num_tasks
        for update skip locked
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;

create or replace function swh_scheduler_schedule_task_run (task_id bigint,
                                                            backend_id text,
                                                            metadata jsonb default '{}'::jsonb,
                                                            ts timestamptz default now())
  returns task_run
  language sql
as $$
  insert into task_run (task, backend_id, metadata, scheduled, status)
    values (task_id, backend_id, metadata, ts, 'scheduled')
  returning *;
$$;

create or replace function swh_scheduler_mktemp_task_run ()
  returns void
  language sql
as $$
  create temporary table tmp_task_run (
    like task_run excluding indexes
  ) on commit drop;
  alter table tmp_task_run
    drop column id,
    drop column status;
$$;

comment on function swh_scheduler_mktemp_task_run () is 'Create a temporary table for bulk task run scheduling';

create or replace function swh_scheduler_schedule_task_run_from_temp ()
  returns void
  language plpgsql
as $$
begin
  insert into task_run (task, backend_id, metadata, scheduled, status)
    select task, backend_id, metadata, scheduled, 'scheduled'
      from tmp_task_run;
  return;
end;
$$;

create or replace function swh_scheduler_start_task_run (backend_id text,
                                                         metadata jsonb default '{}'::jsonb,
                                                         ts timestamptz default now())
  returns task_run
  language sql
as $$
  update task_run
    set started = ts,
        status = 'started',
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_start_task_run.metadata
    where task_run.backend_id = swh_scheduler_start_task_run.backend_id
  returning *;
$$;

create or replace function swh_scheduler_end_task_run (backend_id text,
                                                       status task_run_status,
                                                       metadata jsonb default '{}'::jsonb,
                                                       ts timestamptz default now())
  returns task_run
  language sql
as $$
  update task_run
    set ended = ts,
        status = swh_scheduler_end_task_run.status,
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_end_task_run.metadata
    where task_run.backend_id = swh_scheduler_end_task_run.backend_id
  returning *;
$$;

create or replace function swh_scheduler_compute_new_task_interval (task_type text,
                                                                    current_interval interval,
                                                                    end_status task_run_status)
  returns interval
  language plpgsql
  stable
as $$
declare
  task_type_row task_type%rowtype;
  adjustment_factor float;
begin
  select *
    from task_type
    where type = swh_scheduler_compute_new_task_interval.task_type
  into task_type_row;

  case end_status
  when 'eventful' then
    adjustment_factor := 1/task_type_row.backoff_factor;
  when 'uneventful' then
    adjustment_factor := task_type_row.backoff_factor;
  else
    -- failed or lost task: no backoff.
    adjustment_factor := 1;
  end case;

  return greatest(task_type_row.min_interval,
                  least(task_type_row.max_interval,
                        adjustment_factor * current_interval));
end;
$$;

create or replace function swh_scheduler_update_task_interval ()
  returns trigger
  language plpgsql
as $$
begin
  update task
    set status = 'next_run_not_scheduled',
        current_interval = swh_scheduler_compute_new_task_interval(type, current_interval, new.status),
        next_run = now () + swh_scheduler_compute_new_task_interval(type, current_interval, new.status)
    where id = new.task;
  return null;
end;
$$;

create trigger update_interval_on_task_end
  after update of status on task_run
  for each row
  when (new.status IN ('eventful', 'uneventful', 'failed', 'lost'))
  execute procedure swh_scheduler_update_task_interval ();
