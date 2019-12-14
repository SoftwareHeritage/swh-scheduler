insert into dbversion (version, release, description)
       values (15, now(), 'Work In Progress');

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
