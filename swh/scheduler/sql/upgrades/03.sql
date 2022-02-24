-- SWH Scheduler Schema upgrade
-- from_version: 02
-- to_version: 03
-- description: Fix a bug in handling default intervals for task types

begin;

insert into dbversion (version, release, description)
  values (3, now(), 'Work In Progress');

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, status, current_interval)
    select type, arguments, next_run, 'next_run_not_scheduled',
           (select default_interval from task_type tt where tt.type = tmp_task.type)
      from tmp_task
  returning task.*;
end;
$$;


commit;
