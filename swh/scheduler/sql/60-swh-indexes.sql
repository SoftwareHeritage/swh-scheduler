create index on task(type);
create index on task(next_run);
create index task_args on task using btree ((arguments -> 'args'));
create index task_kwargs on task using gin ((arguments -> 'kwargs'));
create index on task(priority);

create index on task_run(task);
create index on task_run(backend_id);

create index task_run_id_asc_idx on task_run(task asc, started asc);

