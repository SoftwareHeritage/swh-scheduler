create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

comment on table dbversion is 'Schema update tracking';

insert into dbversion (version, release, description)
       values (9, now(), 'Work In Progress');

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

create index on task(type);
create index on task(next_run);
create index task_args on task using btree ((arguments -> 'args'));
create index task_kwargs on task using gin ((arguments -> 'kwargs'));
create index on task(priority);

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

create index on task_run(task);
create index on task_run(backend_id);

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
    drop column status,
    alter column policy drop not null,
    alter column retries_left drop not null;
$$;

comment on function swh_scheduler_mktemp_task () is 'Create a temporary table for bulk task creation';


create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, status, current_interval, policy, retries_left, priority)
    select type, arguments, next_run, 'next_run_not_scheduled',
           (select default_interval from task_type tt where tt.type = t.type),
           coalesce(policy, 'recurring'),
           coalesce(retries_left, (select num_retries from task_type tt where tt.type = t.type), 0),
           coalesce(priority, null)
      from tmp_task t
      where not exists(select 1
                       from task
                       where type=t.type and
                             arguments=t.arguments and
                             policy=t.policy and
                             status='next_run_not_scheduled')
  returning task.*;
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';

create or replace function swh_scheduler_peek_no_priority_tasks (task_type text, ts timestamptz default now(),
                                                                 num_tasks bigint default NULL)
  returns setof task
  language sql
  stable
as $$
select * from task
  where next_run <= ts
        and type = task_type
        and status = 'next_run_not_scheduled'
        and priority is null
  order by next_run
  limit num_tasks
  for update skip locked;
$$;

create or replace function swh_scheduler_peek_priority_tasks (task_type text, ts timestamptz default now(),
                                                              num_tasks_priority bigint default NULL,
                                                              task_priority task_priority default 'normal')
  returns setof task
  language sql
  stable
as $$
  select *
  from task t
  where t.next_run <= ts
        and t.type = task_type
        and t.status = 'next_run_not_scheduled'
        and t.priority = task_priority
  order by t.next_run
  limit ceil(num_tasks_priority * (select ratio from priority_ratio where id = task_priority))
  for update skip locked;
$$;


create or replace function swh_scheduler_peek_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL, num_tasks_priority bigint default NULL)
  returns setof task
  language sql
  stable
as $$
    select * from swh_scheduler_peek_priority_tasks(task_type, ts, num_tasks_priority, 'high')
    union
    select * from swh_scheduler_peek_priority_tasks(task_type, ts, num_tasks_priority, 'normal')
    union
    select * from swh_scheduler_peek_priority_tasks(task_type, ts, num_tasks_priority, 'low')
    union
    select * from swh_scheduler_peek_no_priority_tasks(task_type, ts, num_tasks)
    order by priority, next_run;
$$;

create or replace function swh_scheduler_grab_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL,
                                                           num_tasks_priority bigint default NULL)
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
        select id from swh_scheduler_peek_ready_tasks(task_type, ts, num_tasks, num_tasks_priority)
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

create type task_record as (
    task_id bigint,
    task_policy task_policy,
    task_status task_status,
    task_run_id bigint,
    arguments jsonb,
    type text,
    backend_id text,
    metadata jsonb,
    scheduled timestamptz,
    started timestamptz,
    ended timestamptz
);

create index task_run_id_asc_idx on task_run(task asc, ended asc);

create or replace function swh_scheduler_task_to_archive(
  ts_after timestamptz, ts_before timestamptz, last_id bigint default -1,
  lim bigint default 10)
  returns setof task_record
  language sql stable
as $$
   select t.id as task_id, t.policy as task_policy,
          t.status as task_status, tr.id as task_run_id,
          t.arguments, t.type, tr.backend_id, tr.metadata,
          tr.scheduled, tr.started, tr.ended
   from task_run tr inner join task t on tr.task=t.id
   where ((t.policy = 'oneshot' and t.status ='completed') or
          (t.policy = 'recurring' and t.status ='disabled')) and
          ts_after <= tr.ended  and tr.ended < ts_before and
          t.id > last_id
   order by tr.task, tr.ended
   limit lim;
$$;

comment on function swh_scheduler_task_to_archive is 'Read archivable tasks function';

create or replace function swh_scheduler_delete_archived_tasks(
  task_ids bigint[], task_run_ids bigint[])
  returns void
  language sql
as $$
  -- clean up task_run_ids
  delete from task_run where id in (select * from unnest(task_run_ids));
  -- clean up only tasks whose associated task_run are all cleaned up.
  -- Remaining tasks will stay there and will be cleaned up when
  -- remaining data have been indexed
  delete from task
  where id in (select t.id
               from task t left outer join task_run tr on t.id=tr.task
               where t.id in (select * from unnest(task_ids))
               and tr.task is null);
$$;

comment on function swh_scheduler_delete_archived_tasks is 'Clean up archived tasks function';

create or replace function swh_scheduler_update_task_on_task_end ()
  returns trigger
  language plpgsql
as $$
declare
  cur_task task%rowtype;
  cur_task_type task_type%rowtype;
  adjustment_factor float;
  new_interval interval;
begin
  select * from task where id = new.task into cur_task;
  select * from task_type where type = cur_task.type into cur_task_type;

  case
    when new.status = 'permfailed' then
      update task
        set status = 'disabled'
        where id = cur_task.id;
    when new.status in ('eventful', 'uneventful') then
      case
        when cur_task.policy = 'oneshot' then
          update task
            set status = 'completed'
            where id = cur_task.id;
        when cur_task.policy = 'recurring' then
          if new.status = 'uneventful' then
            adjustment_factor := 1/cur_task_type.backoff_factor;
          else
            adjustment_factor := 1/cur_task_type.backoff_factor;
          end if;
          new_interval := greatest(
            cur_task_type.min_interval,
            least(
              cur_task_type.max_interval,
              adjustment_factor * cur_task.current_interval));
          update task
            set status = 'next_run_not_scheduled',
                next_run = now() + new_interval,
                current_interval = new_interval,
                retries_left = coalesce(cur_task_type.num_retries, 0)
            where id = cur_task.id;
      end case;
    else -- new.status in 'failed', 'lost'
      if cur_task.retries_left > 0 then
        update task
          set status = 'next_run_not_scheduled',
              next_run = now() + coalesce(cur_task_type.retry_delay, interval '1 hour'),
              retries_left = cur_task.retries_left - 1
          where id = cur_task.id;
      else -- no retries left
        case
          when cur_task.policy = 'oneshot' then
            update task
              set status = 'disabled'
              where id = cur_task.id;
          when cur_task.policy = 'recurring' then
            update task
              set status = 'next_run_not_scheduled',
                  next_run = now() + cur_task.current_interval,
                  retries_left = coalesce(cur_task_type.num_retries, 0)
              where id = cur_task.id;
        end case;
      end if; -- retries
  end case;
  return null;
end;
$$;

create trigger update_task_on_task_end
  after update of status on task_run
  for each row
  when (new.status NOT IN ('scheduled', 'started'))
  execute procedure swh_scheduler_update_task_on_task_end ();
