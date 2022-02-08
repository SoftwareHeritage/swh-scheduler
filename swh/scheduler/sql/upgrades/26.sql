-- SWH DB schema upgrade
-- from_version: 25
-- to_version: 26
-- description: Add new functions to peek/grab tasks (with any priority) ready to be
-- scheduled.

insert into dbversion (version, release, description)
       values (26, now(), 'Work In Progress');

create or replace function swh_scheduler_peek_any_ready_priority_tasks (
    task_type text, ts timestamptz default now(),
    num_tasks bigint default NULL
  )
  returns setof task
  language sql stable
as $$
  select *
  from task t
  where t.next_run <= ts
        and t.type = task_type
        and t.status = 'next_run_not_scheduled'
        and t.priority is not null
  order by t.next_run
  limit num_tasks
  for update skip locked;
$$;

comment on function swh_scheduler_peek_any_ready_priority_tasks(text, timestamptz, bigint)
is 'List tasks with any priority ready for scheduling';

create or replace function swh_scheduler_grab_any_ready_priority_tasks (
    task_type text, ts timestamptz default now(),
    num_tasks bigint default NULL
  )
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
        select id from swh_scheduler_peek_any_ready_priority_tasks(
          task_type, ts, num_tasks
        )
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;

comment on function swh_scheduler_grab_any_ready_priority_tasks (text, timestamptz, bigint)
is 'Grab any priority tasks ready for scheduling and change their status';
