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
  limit num_tasks_priority;
$$;

comment on function swh_scheduler_peek_tasks_with_priority(text, timestamptz, bigint, task_priority)
is 'Retrieve tasks with a given priority';


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

create or replace function swh_scheduler_grab_any_ready_priority_tasks (
    task_type text, ts timestamptz default now(),
    num_tasks bigint default NULL
  )
  returns setof task
  language sql
as $$
  update task
    set status='next_run_scheduled'
    from (
        select id from swh_scheduler_peek_any_ready_priority_tasks(
          task_type, ts, num_tasks
        )
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;

comment on function swh_scheduler_grab_any_ready_priority_tasks (text, timestamptz, bigint)
is 'Grab any priority tasks ready for scheduling and change their status';


create or replace function swh_scheduler_schedule_task_run (task_id bigint,
                                                            backend_id text,
                                                            metadata jsonb default '{}'::jsonb,
                                                            ts timestamptz default now())
  returns task_run
  language sql
as $$
  insert into task_run (task, backend_id, metadata, scheduled, status)
    values (task_id, backend_id, metadata, ts, 'scheduled')
  returning *;
$$;

create or replace function swh_scheduler_mktemp_task_run ()
  returns void
  language sql
as $$
  create temporary table tmp_task_run (
    like task_run excluding indexes
  ) on commit drop;
  alter table tmp_task_run
    drop column id,
    drop column status;
$$;

comment on function swh_scheduler_mktemp_task_run () is 'Create a temporary table for bulk task run scheduling';

create or replace function swh_scheduler_schedule_task_run_from_temp ()
  returns void
  language plpgsql
as $$
begin
  insert into task_run (task, backend_id, metadata, scheduled, status)
    select task, backend_id, metadata, scheduled, 'scheduled'
      from tmp_task_run;
  return;
end;
$$;

create or replace function swh_scheduler_start_task_run (backend_id text,
                                                         metadata jsonb default '{}'::jsonb,
                                                         ts timestamptz default now())
  returns task_run
  language sql
as $$
  update task_run
    set started = ts,
        status = 'started',
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_start_task_run.metadata
    where task_run.backend_id = swh_scheduler_start_task_run.backend_id
  returning *;
$$;

create or replace function swh_scheduler_end_task_run (backend_id text,
                                                       status task_run_status,
                                                       metadata jsonb default '{}'::jsonb,
                                                       ts timestamptz default now())
  returns task_run
  language sql
as $$
  update task_run
    set ended = ts,
        status = swh_scheduler_end_task_run.status,
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_end_task_run.metadata
    where task_run.backend_id = swh_scheduler_end_task_run.backend_id
  returning *;
$$;

create type task_record as (
    task_id bigint,
    task_policy task_policy,
    task_status task_status,
    task_run_id bigint,
    arguments jsonb,
    type text,
    backend_id text,
    metadata jsonb,
    scheduled timestamptz,
    started timestamptz,
    ended timestamptz,
    task_run_status task_run_status
);

create or replace function swh_scheduler_task_to_archive(
  ts_after timestamptz, ts_before timestamptz, last_id bigint default -1,
  lim bigint default 10)
  returns setof task_record
  language sql stable
as $$
   select t.id as task_id, t.policy as task_policy,
          t.status as task_status, tr.id as task_run_id,
          t.arguments, t.type, tr.backend_id, tr.metadata,
          tr.scheduled, tr.started, tr.ended, tr.status as task_run_status
   from task_run tr inner join task t on tr.task=t.id
   where ((t.policy = 'oneshot' and t.status in ('completed', 'disabled')) or
          (t.policy = 'recurring' and t.status = 'disabled')) and
          ((ts_after <= tr.started and tr.started < ts_before) or
           (tr.started is null and (ts_after <= tr.scheduled and tr.scheduled < ts_before))) and
          t.id >= last_id
   order by tr.task, tr.started
   limit lim;
$$;

comment on function swh_scheduler_task_to_archive (timestamptz, timestamptz, bigint, bigint) is 'Read archivable tasks function';

create or replace function swh_scheduler_delete_archived_tasks(
  task_ids bigint[], task_run_ids bigint[])
  returns void
  language sql
as $$
  -- clean up task_run_ids
  delete from task_run where id in (select * from unnest(task_run_ids));
  -- clean up only tasks whose associated task_run are all cleaned up.
  -- Remaining tasks will stay there and will be cleaned up when
  -- remaining data have been indexed
  delete from task
  where id in (select t.id
               from task t left outer join task_run tr on t.id=tr.task
               where t.id in (select * from unnest(task_ids))
               and tr.task is null);
$$;

comment on function swh_scheduler_delete_archived_tasks(bigint[], bigint[]) is 'Clean up archived tasks function';

create or replace function swh_scheduler_update_task_on_task_end ()
  returns trigger
  language plpgsql
as $$
declare
  cur_task task%rowtype;
  cur_task_type task_type%rowtype;
  adjustment_factor float;
  new_interval interval;
begin
  select * from task where id = new.task into cur_task;
  select * from task_type where type = cur_task.type into cur_task_type;

  case
    when new.status = 'permfailed' then
      update task
        set status = 'disabled'
        where id = cur_task.id;
    when new.status in ('eventful', 'uneventful') then
      case
        when cur_task.policy = 'oneshot' then
          update task
            set status = 'completed'
            where id = cur_task.id;
        when cur_task.policy = 'recurring' then
          if new.status = 'uneventful' then
            adjustment_factor := 1/cur_task_type.backoff_factor;
          else
            adjustment_factor := 1/cur_task_type.backoff_factor;
          end if;
          new_interval := greatest(
            cur_task_type.min_interval,
            least(
              cur_task_type.max_interval,
              adjustment_factor * cur_task.current_interval));
          update task
            set status = 'next_run_not_scheduled',
                next_run = new.ended + new_interval,
                current_interval = new_interval,
                retries_left = coalesce(cur_task_type.num_retries, 0)
            where id = cur_task.id;
      end case;
    else -- new.status in 'failed', 'lost'
      if cur_task.retries_left > 0 then
        update task
          set status = 'next_run_not_scheduled',
              next_run = new.ended + coalesce(cur_task_type.retry_delay, interval '1 hour'),
              retries_left = cur_task.retries_left - 1
          where id = cur_task.id;
      else -- no retries left
        case
          when cur_task.policy = 'oneshot' then
            update task
              set status = 'disabled'
              where id = cur_task.id;
          when cur_task.policy = 'recurring' then
            update task
              set status = 'next_run_not_scheduled',
                  next_run = new.ended + cur_task.current_interval,
                  retries_left = coalesce(cur_task_type.num_retries, 0)
              where id = cur_task.id;
        end case;
      end if; -- retries
  end case;
  return null;
end;
$$;

create trigger update_task_on_task_end
  after update of status on task_run
  for each row
  when (new.status NOT IN ('scheduled', 'started'))
  execute procedure swh_scheduler_update_task_on_task_end ();


create or replace function update_metrics(lister_id uuid default NULL, ts timestamptz default now())
  returns setof scheduler_metrics
  language sql
as $$
  insert into scheduler_metrics (
    lister_id, visit_type, last_update,
    origins_known, origins_enabled,
    origins_never_visited, origins_with_pending_changes
  )
    select
      lo.lister_id, lo.visit_type, coalesce(ts, now()) as last_update,
      count(*) as origins_known,
      count(*) filter (where enabled) as origins_enabled,
      count(*) filter (where
        enabled and last_snapshot is NULL
      ) as origins_never_visited,
      count(*) filter (where
        enabled and lo.last_update > last_successful
      ) as origins_with_pending_changes
    from listed_origins lo
    left join origin_visit_stats ovs using (url, visit_type)
    where
      -- update only for the requested lister
      update_metrics.lister_id = lo.lister_id
      -- or for all listers if the function argument is null
      or update_metrics.lister_id is null
    group by (lister_id, visit_type)
  on conflict (lister_id, visit_type) do update
    set
      last_update = EXCLUDED.last_update,
      origins_known = EXCLUDED.origins_known,
      origins_enabled = EXCLUDED.origins_enabled,
      origins_never_visited = EXCLUDED.origins_never_visited,
      origins_with_pending_changes = EXCLUDED.origins_with_pending_changes
  returning *
$$;

comment on function update_metrics(uuid, timestamptz) is 'Update metrics for the given lister_id';
