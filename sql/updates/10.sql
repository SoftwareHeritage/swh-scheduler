-- SWH Scheduler Schema upgrade
-- from_version: 09
-- to_version: 10
-- description: Schedule task with priority

insert into dbversion (version, release, description)
values (10, now(), 'Work In Progress');

drop type task_record cascade;
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

drop index task_run_id_asc_idx;
create index task_run_id_started_asc_idx on task_run(task asc, started asc);

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
          ((ts_after <= tr.started and tr.started < ts_before) or tr.started is null) and
          t.id > last_id
   order by tr.task, tr.started
   limit lim;
$$;

comment on function swh_scheduler_task_to_archive is 'Read archivable tasks function';
