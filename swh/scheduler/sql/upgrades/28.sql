-- SWH DB schema upgrade
-- from_version: 27
-- to_version: 28
-- description: Drop row locking for runner queries

insert into dbversion (version, release, description)
       values (28, now(), 'Work In Progress');


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
  limit num_tasks;
$$;

comment on function swh_scheduler_peek_no_priority_tasks (text, timestamptz, bigint)
is 'Retrieve tasks without priority';


create or replace function swh_scheduler_peek_tasks_with_priority (task_type text, ts timestamptz default now(),
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
  limit num_tasks_priority;
$$;

comment on function swh_scheduler_peek_tasks_with_priority(text, timestamptz, bigint, task_priority)
is 'Retrieve tasks with a given priority';


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
  limit num_tasks;
$$;

comment on function swh_scheduler_peek_any_ready_priority_tasks(text, timestamptz, bigint)
is 'List tasks with any priority ready for scheduling';
