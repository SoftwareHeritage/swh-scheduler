insert into dbversion (version, release, description)
       values (24, now(), 'Work In Progress');

alter table origin_visit_stats add column last_scheduled timestamptz;
comment on column origin_visit_stats.last_scheduled is 'Time when this origin was scheduled to be visited last';

-- no need for a proper migration script of this last_schedules column: this
-- have not been published or deployed; just drop it
alter table listed_origins drop column last_scheduled;
