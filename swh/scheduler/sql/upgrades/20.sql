
insert into dbversion (version, release, description)
       values (20, now(), 'Work In Progress');

create index on listed_origins (visit_type, last_scheduled);
drop index listed_origins_last_scheduled_idx;
