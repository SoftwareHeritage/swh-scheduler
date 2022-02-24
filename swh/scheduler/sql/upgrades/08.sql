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

drop function swh_scheduler_delete_archive_tasks(bigint[]);

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

comment on function swh_scheduler_delete_archived_tasks is 'Clean up archived tasks function';
