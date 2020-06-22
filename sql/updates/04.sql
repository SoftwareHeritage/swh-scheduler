-- SWH Scheduler Schema upgrade
-- from_version: 03
-- to_version: 04
-- description: Add a maximum queue length to the task types in the scheduler

begin;

insert into dbversion (version, release, description)
values (4, now(), 'Work In Progress');

alter table task_type add column max_queue_length bigint;
comment on column task_type.max_queue_length is 'Maximum length of the queue for this type of tasks';


drop function swh_scheduler_peek_ready_tasks (timestamptz, bigint);
drop function swh_scheduler_grab_ready_tasks (timestamptz, bigint);

create or replace function swh_scheduler_peek_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL)
  returns setof task
  language sql
  stable
as $$
select * from task
  where next_run <= ts
        and type = task_type
        and status = 'next_run_not_scheduled'
  order by next_run
  limit num_tasks;
$$;

create or replace function swh_scheduler_grab_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL)
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
      select id from task
        where next_run <= ts
              and type = task_type
              and status='next_run_not_scheduled'
        order by next_run
        limit num_tasks
        for update skip locked
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;


commit;
