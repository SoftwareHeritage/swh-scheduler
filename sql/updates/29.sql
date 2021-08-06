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

alter table origin_visit_stats
add column successive_visits int not null default 1;

comment on column origin_visit_stats.next_visit_queue_position is 'Time at which some new objects are expected to be found';
comment on column origin_visit_stats.next_position_offset is 'Duration that we expect to wait between visits of this origin';
comment on column origin_visit_stats.successive_visits is 'number of successive visits with the same status';

create table visit_scheduler_queue_position (
  visit_type text not null,
  position timestamptz not null,

  primary key (visit_type)
);

comment on table visit_scheduler_queue_position is 'Current queue position for the recurrent visit scheduler';
comment on column visit_scheduler_queue_position.visit_type is 'Visit type';
comment on column visit_scheduler_queue_position.position is 'Current position for the runner of this visit type';
