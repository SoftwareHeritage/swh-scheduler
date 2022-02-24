-- SWH Scheduler Schema upgrade
-- from_version: 05
-- to_version: 06
-- description: relax constraints on intervals for one-shot tasks

begin;

insert into dbversion (version, release, description)
       values (6, now(), 'Work In Progress');


alter table task_type
    alter column default_interval drop not null,
    alter column min_interval drop not null,
    alter column max_interval drop not null,
    alter column backoff_factor drop not null;

alter table task
    alter column current_interval drop not null,
    add constraint task_check check (policy <> 'recurring' or current_interval is not null);

commit;

