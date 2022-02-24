-- SWH Scheduler Schema upgrade
-- from_version: 11
-- to_version: 12
-- description: Upgrade scheduler create_tasks routine

insert into dbversion (version, release, description)
  values (12, now(), 'Work In Progress');

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  -- update the default values in one go
  -- this is separated from the insert/select to avoid too much
  -- juggling
  update tmp_task t
  set current_interval = tt.default_interval,
      retries_left = coalesce(retries_left, tt.num_retries, 0)
  from task_type tt
  where tt.type=t.type;

  insert into task (type, arguments, next_run, status, current_interval, policy,
                    retries_left, priority)
    select type, arguments, next_run, status, current_interval, policy,
           retries_left, priority
    from tmp_task t
    where not exists(select 1
                     from task
                     where type = t.type and
                           arguments->'args' = t.arguments->'args' and
                           arguments->'kwargs' = t.arguments->'kwargs' and
                           policy = t.policy and
                           priority is not distinct from t.priority and
                           status = t.status);

  return query
    select distinct t.*
    from tmp_task tt inner join task t on (
      tt.type = t.type and
      tt.arguments->'args' = t.arguments->'args' and
      tt.arguments->'kwargs' = t.arguments->'kwargs' and
      tt.policy = t.policy and
      tt.priority is not distinct from t.priority and
      tt.status = t.status
    );
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';

