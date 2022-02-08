insert into dbversion (version, release, description)
       values (14, now(), 'Work In Progress');

drop index task_args;
drop index task_kwargs;

create index on task using btree(type, md5(arguments::text));

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
                           status = t.status);

  return query
    select distinct t.*
    from tmp_task tt inner join task t on (
      tt.type = t.type and
      md5(tt.arguments::text) = md5(t.arguments::text) and
      tt.arguments = t.arguments and
      tt.policy = t.policy and
      tt.priority is not distinct from t.priority and
      tt.status = t.status
    );
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';
