-- SWH Scheduler Schema upgrade
-- from_version: 08
-- to_version: 09
-- description: Schedule task with priority

insert into dbversion (version, release, description)
values (9, now(), 'Work In Progress');

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

alter table task add column priority task_priority references priority_ratio(id);
comment on column task.priority is 'Policy of the given task';

drop function swh_scheduler_peek_ready_tasks(text, timestamptz, bigint);
drop function swh_scheduler_grab_ready_tasks(text, timestamptz, bigint);

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

comment on function swh_scheduler_peek_no_priority_tasks (text, timestamptz, bigint)
is 'Retrieve tasks without priority';

create or replace function swh_scheduler_nb_priority_tasks(num_tasks_priority bigint, task_priority task_priority)
  returns numeric
  language sql stable
as $$
  select ceil(num_tasks_priority * (select ratio from priority_ratio where id = task_priority)) :: numeric
$$;

comment on function swh_scheduler_nb_priority_tasks (bigint, task_priority)
is 'Given a priority task and a total number, compute the number of tasks to read';

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
  limit num_tasks_priority
  for update skip locked;
$$;

comment on function swh_scheduler_peek_tasks_with_priority(text, timestamptz, bigint, task_priority)
is 'Retrieve tasks with a given priority';

create or replace function swh_scheduler_peek_priority_tasks (task_type text, ts timestamptz default now(),
                                                              num_tasks_priority bigint default NULL)
  returns setof task
  language plpgsql
as $$
declare
  r record;
  count_row bigint;
  nb_diff bigint;
  nb_high bigint;
  nb_normal bigint;
  nb_low bigint;
begin
    -- expected values to fetch
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'high') into nb_high;
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'normal') into nb_normal;
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'low') into nb_low;
    nb_diff := 0;
    count_row := 0;

    for r in select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_high, 'high')
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < nb_high then
        nb_normal := nb_normal + nb_high - count_row;
    end if;

    count_row := 0;
    for r in select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_normal, 'normal')
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < nb_normal then
        nb_low := nb_low + nb_normal - count_row;
    end if;

    return query select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_low, 'low');
end
$$;

comment on function swh_scheduler_peek_priority_tasks(text, timestamptz, bigint)
is 'Retrieve priority tasks';

create or replace function swh_scheduler_peek_ready_tasks (task_type text, ts timestamptz default now(),
                                                           num_tasks bigint default NULL, num_tasks_priority bigint default NULL)
  returns setof task
  language plpgsql
as $$
declare
  r record;
  count_row bigint;
  nb_diff bigint;
  nb_tasks bigint;
begin
    count_row := 0;

    for r in select * from swh_scheduler_peek_priority_tasks(task_type, ts, num_tasks_priority)
             order by priority, next_run
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < num_tasks_priority then
       nb_tasks := num_tasks + num_tasks_priority - count_row;
    else
       nb_tasks := num_tasks;
    end if;

    for r in select * from swh_scheduler_peek_no_priority_tasks(task_type, ts, nb_tasks)
             order by priority, next_run
    loop
        return next r;
    end loop;

    return;
end
$$;

comment on function swh_scheduler_peek_ready_tasks(text, timestamptz, bigint, bigint)
is 'Retrieve tasks with/without priority in order';

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

comment on function swh_scheduler_grab_ready_tasks (text, timestamptz, bigint, bigint)
is 'Grab tasks ready for scheduling and change their status';

create index on task(priority);

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, status, current_interval, policy, retries_left, priority)
    select type, arguments, next_run, 'next_run_not_scheduled',
           (select default_interval from task_type tt where tt.type = tmp_task.type),
           coalesce(policy, 'recurring'),
           coalesce(retries_left, (select num_retries from task_type tt where tt.type = tmp_task.type), 0),
           coalesce(priority, null)
      from tmp_task
  returning task.*;
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';
