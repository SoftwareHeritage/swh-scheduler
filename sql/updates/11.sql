-- SWH Scheduler Schema upgrade
-- from_version: 10
-- to_version: 11
-- description: Upgrade scheduler create_tasks routine

insert into dbversion (version, release, description)
values (11, now(), 'Work In Progress');

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  -- insert new task, dropping the existing one
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
                             status='next_run_not_scheduled');

  -- return all ids (even the existing ones)
  return query
    select t.*
    from tmp_task tt inner join task t on (
      t.type=tt.type and
      t.arguments=tt.arguments and
      t.policy=tt.policy and
      t.status='next_run_not_scheduled');
end;
$$;
