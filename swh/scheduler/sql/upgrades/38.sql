insert into dbversion (version, release, description)
       values (38, now(), 'Work In Progress');

alter table listers add column last_listing_finished_at timestamptz;
alter table listers add column first_visits_queue_prefix text;
alter table listers add column first_visits_scheduled_at timestamptz;
comment on column listers.last_listing_finished_at is 'Timestamp at which the last execution of the lister finished';
comment on column listers.first_visits_queue_prefix is 'Optional prefix of message queue names to schedule first visits with high priority';
comment on column listers.first_visits_scheduled_at is 'Timestamp at which all first visits of listed origins have been scheduled with high priority';