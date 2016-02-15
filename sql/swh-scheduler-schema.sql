create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

insert into dbversion (version, release, description)
       values (1, now(), 'Work In Progress');

create table task_type (
  type text primary key,
  description text not null,
  task_name text not null,
  default_interval interval not null,
  min_interval interval not null,
  max_interval interval not null,
  backoff_factor float not null
);

create type task_status as enum ('pending', 'scheduled', 'disabled');

create table task (
  id bigserial primary key,
  type text not null references task_type(type),
  arguments jsonb not null,
  next_run timestamptz not null,
  current_interval interval not null,
  status task_status not null
);

create index on task(type);
create index on task(next_run);
create index on task using btree ((arguments -> 'args'));
create index on task using gin ((arguments -> 'kwargs'));

create table task_run (
  id bigserial primary key,
  task bigint not null references task(id),
  backend_id text,
  scheduled timestamptz,
  started timestamptz,
  ended timestamptz,
  metadata jsonb,
  eventful boolean
);

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
    drop column status;
$$;

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, current_interval, status)
    select type, arguments, next_run, (select default_interval from task_type tt where tt.type = type), 'pending'
      from tmp_task
  returning task.*;
end;
$$;

create or replace function swh_scheduler_peek_pending_tasks (ts timestamptz default now(), num_tasks bigint default NULL)
  returns setof task
  language sql
  stable
as $$
select * from task
  where next_run <= ts
  and status='pending'
  order by next_run
  limit num_tasks;
$$;

create or replace function swh_scheduler_grab_pending_tasks (ts timestamptz default now(), num_tasks bigint default NULL)
  returns setof task
  language sql
as $$
  update task
    set status='scheduled'
    from (
      select id from task
        where next_run <= ts
              and status='pending'
        order by next_run
        limit num_tasks
        for update skip locked
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;

create or replace function swh_scheduler_schedule_task_run (task_id bigint, backend_id text, metadata jsonb default '{}'::jsonb)
  returns task_run
  language sql
as $$
  insert into task_run (task, backend_id, metadata, scheduled)
    values (task_id, backend_id, metadata, now())
  returning *;
$$;

create or replace function swh_scheduler_start_task_run (backend_id text, metadata jsonb default '{}'::jsonb)
  returns task_run
  language sql
as $$
  update task_run
    set started = now (),
        metadata = task_run.metadata || swh_scheduler_start_task_run.metadata
    where task_run.backend_id = swh_scheduler_start_task_run.backend_id
  returning *;
$$;

create or replace function swh_scheduler_end_task_run (backend_id text, eventful boolean, metadata jsonb default '{}'::jsonb)
  returns task_run
  language sql
as $$
  update task_run
    set ended = now (),
        eventful = swh_scheduler_end_task_run.eventful,
        metadata = task_run.metadata || swh_scheduler_end_task_run.metadata
    where task_run.backend_id = swh_scheduler_end_task_run.backend_id
  returning *;
$$;

create or replace function swh_scheduler_compute_new_task_interval (task_type text, current_interval interval, eventful boolean)
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

  if eventful then
    adjustment_factor := 1/task_type_row.backoff_factor;
  else
    adjustment_factor := task_type_row.backoff_factor;
  end if;

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
    set status = 'pending',
        current_interval = swh_scheduler_compute_new_task_interval(type, current_interval, NEW.eventful),
        next_run = now () + swh_scheduler_compute_new_task_interval(type, current_interval, NEW.eventful)
    where id = NEW.task;
  return NULL;
end;
$$;

create trigger update_interval_on_task_end
  after update of ended, eventful on task_run
  for each row
  execute procedure swh_scheduler_update_task_interval ();
