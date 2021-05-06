create index on task(type);
create index on task(next_run);

-- used for quick equality checking
create index on task using btree(type, md5(arguments::text));

create index on task(priority);

create index on task_run(task);
create index on task_run(backend_id);

create index task_run_id_asc_idx on task_run(task asc, started asc);

create index on task(type, next_run)
where status = 'next_run_not_scheduled'::task_status;


-- lister schema
create unique index on listers (name, instance_name);

-- listed origins
create index on listed_origins (url, visit_type);

-- visit stats
create index on origin_visit_stats (url, visit_type);
-- XXX probably add indexes on most (visit_type, last_xxx) couples
