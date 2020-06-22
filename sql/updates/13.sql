insert into dbversion (version, release, description)
       values (13, now(), 'Work In Progress');

-- comments for columns of table dbversion
comment on column dbversion.version is 'SQL schema version';
comment on column dbversion.release is 'Version deployment timestamp';
comment on column dbversion.description is 'Version description';

-- comments for columns of table task
comment on column task.id is 'Task Identifier';
comment on column task.type is 'References task_type table';
comment on column task.status is 'Task status (''next_run_not_scheduled'', ''next_run_scheduled'', ''completed'', ''disabled'')';

-- comments for columns of table task_run
comment on column task_run.id is 'Task run identifier';
comment on column task_run.task is 'References task table';
comment on column task_run.scheduled is 'Scheduled run time for task';
comment on column task_run.started is 'Task starting time';
comment on column task_run.ended is 'Task ending time';
