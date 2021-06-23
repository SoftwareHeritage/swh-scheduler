-- SWH DB schema upgrade
-- from_version: 28
-- to_version: 29
-- description:

insert into dbversion (version, release, description)
       values (29, now(), 'Work In Progress');

alter table origin_visit_stats
add column next_visit_queue_position timestamptz;

alter table origin_visit_stats
add column next_position_offset int not null default 4;

comment on column origin_visit_stats.next_visit_queue_position is 'Time at which some new objects are expected to be found';
comment on column origin_visit_stats.next_position_offset is 'Duration that we expect to wait between visits of this origin';
