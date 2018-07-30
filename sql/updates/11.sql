-- SWH Scheduler Schema upgrade
-- from_version: 10
-- to_version: 11
-- description: Upgrade scheduler create_tasks routine

insert into dbversion (version, release, description)
values (11, now(), 'Work In Progress');

create or replace function swh_scheduler_mktemp_task ()
  returns void
  language sql
as $$
  create temporary table tmp_task (
    like task excluding indexes
  ) on commit drop;
  alter table tmp_task
    alter column retries_left drop not null,
    drop column id;
$$;

comment on function swh_scheduler_mktemp_task () is 'Create a temporary table for bulk task creation';

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
                           arguments = t.arguments and
                           policy = t.policy and
                           ((priority is null and t.priority is null)
                            or priority = t.priority) and
                           status = t.status);

  return query
    select distinct t.*
    from tmp_task tt inner join task t on (
      t.type = tt.type and
      t.arguments = tt.arguments and
      t.status = tt.status and
      ((t.priority is null and tt.priority is null)
       or t.priority=tt.priority) and
       t.policy=tt.policy
    );
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';
