-- SWH DB schema upgrade
-- from_version: 26
-- to_version: 27
-- description: Clean up no longer used stored procedure

insert into dbversion (version, release, description)
       values (27, now(), 'Work In Progress');

drop function swh_scheduler_peek_ready_tasks (text, timestamptz, bigint, bigint);
drop function swh_scheduler_peek_priority_tasks (text, timestamptz, bigint);
-- delete old signature function
drop function swh_scheduler_grab_ready_tasks (text, timestamptz, bigint, bigint);

create or replace function swh_scheduler_grab_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL)
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
        select id from swh_scheduler_peek_no_priority_tasks(task_type, ts, num_tasks)
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;

comment on function swh_scheduler_grab_ready_tasks (text, timestamptz, bigint)
is 'Grab (no priority) tasks ready for scheduling and change their status';
