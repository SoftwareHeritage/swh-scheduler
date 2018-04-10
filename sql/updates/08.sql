-- SWH Scheduler Schema upgrade
-- from_version: 07
-- to_version: 08
-- description: Improve task to archive filtering function

insert into dbversion (version, release, description)
values (8, now(), 'Work In Progress');

drop function swh_scheduler_task_to_archive(timestamptz, bigint, bigint);

create or replace function swh_scheduler_task_to_archive(
  ts_after timestamptz, ts_before timestamptz, last_id bigint default -1,
  lim bigint default 10)
  returns setof task_record
  language sql stable
as $$
   select t.id as task_id, t.policy as task_policy,
          t.status as task_status, tr.id as task_run_id,
          t.arguments, t.type, tr.backend_id, tr.metadata,
          tr.scheduled, tr.started, tr.ended
   from task_run tr inner join task t on tr.task=t.id
   where ((t.policy = 'oneshot' and t.status ='completed') or
          (t.policy = 'recurring' and t.status ='disabled')) and
          ts_after <= tr.ended  and tr.ended < ts_before and
          t.id > last_id
   order by tr.task, tr.ended
   limit lim;
$$;

comment on function swh_scheduler_task_to_archive is 'Read archivable tasks function';
