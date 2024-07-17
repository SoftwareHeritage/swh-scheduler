-- SWH DB schema upgrade
-- from_version: 35
-- to_version: 36
-- description: Remove no longer used priority_ratio table
--              and swh_scheduler_nb_priority_tasks function

insert into dbversion (version, release, description)
       values (36, now(), 'Work In Progress');

alter table task drop constraint task_priority_fk;

drop table priority_ratio;

drop function swh_scheduler_nb_priority_tasks;
