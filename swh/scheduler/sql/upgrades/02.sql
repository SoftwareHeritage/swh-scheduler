-- SWH Scheduler Schema upgrade
-- from_version: 01
-- to_version: 02
-- description: Allow mass-creation of task runs

begin;

insert into dbversion (version, release, description)
  values (2, now(), 'Work In Progress');

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

commit;
