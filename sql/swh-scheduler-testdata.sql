begin;

insert into task_type
  (type, task_name, description, min_interval, max_interval, default_interval, backoff_factor)
values
  ('git-updater', 'swh.loader.git.tasks.UpdateGitRepository', 'Git repository updater', '12 hours'::interval, '30 days'::interval, '1 day'::interval, 2);


select swh_scheduler_mktemp_task ();


insert into tmp_task
  (type, arguments, next_run)
values
  ('git-updater',
   '{"args":["git://git.kernel.org/pub/scm/linux/kernel/git/torvalds/linux.git"], "kwargs":{}}'::jsonb,
   now());

select * from swh_scheduler_create_tasks_from_temp ();

commit;


select * from swh_scheduler_grab_pending_tasks ();
select * from swh_scheduler_schedule_task_run(1, 'foo');
select * from swh_scheduler_start_task_run('foo', '{"worker": "worker01"}');
select * from swh_scheduler_end_task_run('foo', true, '{"logs": "foobarbaz"}');

select * from task;
select * from task_run;
