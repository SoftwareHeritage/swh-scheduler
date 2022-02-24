-- SWH Scheduler Schema upgrade
-- from_version: 06
-- to_version: 07
-- description: Archive 'oneshot' and disabled 'recurring' tasks (status = 'disabled')

insert into dbversion (version, release, description)
values (7, now(), 'Work In Progress');

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
    ended timestamptz
);

create index task_run_id_asc_idx on task_run(task asc, ended asc);

create or replace function swh_scheduler_task_to_archive(
  ts timestamptz, last_id bigint default -1, lim bigint default 10)
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
          tr.ended < ts and
          t.id > last_id
   order by tr.task, tr.ended
   limit lim;
$$;

comment on function swh_scheduler_task_to_archive is 'Read archivable tasks function';

create or replace function swh_scheduler_delete_archive_tasks(
  task_ids bigint[])
  returns void
  language sql
as $$
  delete from task_run where task in (select * from unnest(task_ids));
  delete from task where id in (select * from unnest(task_ids));
$$;

comment on function swh_scheduler_delete_archive_tasks is 'Clean up archived tasks function';
