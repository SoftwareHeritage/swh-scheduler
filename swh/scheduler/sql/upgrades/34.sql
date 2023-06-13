-- SWH DB schema upgrade
-- from_version: 33
-- to_version: 34
-- description: Fix task creation with custom next_run

insert into dbversion (version, release, description)
       values (34, now(), 'Work In Progress');

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
                           md5(arguments::text) = md5(t.arguments::text) and
                           arguments = t.arguments and
                           policy = t.policy and
                           priority is not distinct from t.priority and
                           status = t.status and
                           (policy != 'oneshot' or next_run = t.next_run));

  return query
    select distinct t.*
    from tmp_task tt inner join task t on (
      tt.type = t.type and
      md5(tt.arguments::text) = md5(t.arguments::text) and
      tt.arguments = t.arguments and
      tt.policy = t.policy and
      tt.priority is not distinct from t.priority and
      tt.status = t.status and
      tt.next_run = t.next_run
    );
end;
$$;
