create index on task(type);
create index on task(next_run);

-- used for quick equality checking
create index on task using btree(type, md5(arguments::text));

create index on task(priority);

create index on task_run(task);
create index on task_run(backend_id);

create index task_run_id_asc_idx on task_run(task asc, started asc);

