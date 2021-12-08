-- SWH DB schema upgrade
-- from_version: 30
-- to_version: 31
-- description: make next_visit_queue_position integers

insert into dbversion (version, release, description)
       values (31, now(), 'Work In Progress');


alter table
    origin_visit_stats
  alter column
    next_visit_queue_position
    type bigint
    using extract(epoch from next_visit_queue_position);

comment on column origin_visit_stats.next_visit_queue_position is
  'Position in the global per origin-type queue at which some new objects are expected to be found';

alter table
    visit_scheduler_queue_position
  alter column
    "position"
    type bigint
    using extract(epoch from "position");
