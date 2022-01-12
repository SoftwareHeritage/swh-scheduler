-- SWH DB schema upgrade
-- from_version: 32
-- to_version: 33
-- description: Archive old task/task_run tasks

insert into dbversion (version, release, description)
       values (33, now(), 'Work In Progress');

--------------------
-- Schema adaptation
--------------------

-- This takes care of copying everything that matters from the old table to the new
-- (sequences, comments...). Keeping what could slow down the copy on the side (indexes,
-- constraints). Those will be installed at the end of the migration.

create table new_task (
  like task
  including all
  excluding indexes
  excluding constraints
);

create table new_task_run (
  like task_run
  including all
  excluding indexes
  excluding constraints
);

------------------------
-- Actual data migration
------------------------

insert into new_task(id, type, arguments, next_run, current_interval, status, policy,
  retries_left, priority)
select id, type, arguments, next_run, current_interval, status, policy,
  retries_left, priority
from task
where (
    policy='recurring' and (type = 'load-nixguix' or
                            type like 'list-%' or
                            type like 'index-%')
  ) or (
    policy = 'oneshot' and next_run > now() - interval '2 months'
  );

-- Keep only the necessary task runs (reusing the previous filtered task entries to
-- create a consistency dataset faster)
insert into new_task_run
select id, task, backend_id, scheduled, started, ended, metadata, status
from task_run where task in (
  select distinct id from new_task
);

select last_value from task_id_seq;
select last_value from task_run_id_seq;

-----------
-- Renaming
-----------

-- Rename current tables to archive_ prefixed names tables
alter table task rename to archive_task;
alter table task_run rename to archive_task_run;

-- Rename new tables to standard tables
alter table new_task rename to task;
alter table new_task_run rename to task_run;

-----------------------------
-- Check sequence consistency
-----------------------------

select last_value from task_id_seq;
select last_value from task_run_id_seq;

--------------
-- PKs and FKs
--------------

alter table task
add primary key(id);

alter table task
alter column type set not null;

alter table task
add constraint task_type_fk
foreign key (type) references task_type (type);

alter table task
add constraint task_priority_fk
foreign key (priority) references priority_ratio (id);

alter table task
add constraint task_check_policy
check (policy <> 'recurring' or current_interval is not null)
not valid;

alter table task
  validate constraint task_check_policy;

alter table task_run
add primary key(id);

alter table task_run
alter column status set not null,
alter column status set default 'scheduled';

alter table task_run
alter column task set not null,
add constraint task_id_fk
foreign key (task) references task (id);

----------
-- Indexes
----------

create index on task(type);
create index on task(next_run);

create index on task using btree(type, md5(arguments::text));
create index on task(priority);

create index on task_run(task);
create index on task_run(backend_id);

create index on task_run(task asc, started asc);

create index on task(type, next_run)
where status = 'next_run_not_scheduled'::task_status;

----------
-- Trigger
----------

-- Drop trigger on archive table
drop trigger update_task_on_task_end on archive_task_run;

create trigger update_task_on_task_end
  after update of status on task_run
  for each row
  when (new.status NOT IN ('scheduled', 'started'))
  execute procedure swh_scheduler_update_task_on_task_end ();
