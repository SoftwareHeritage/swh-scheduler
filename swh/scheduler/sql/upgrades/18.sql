
insert into dbversion (version, release, description)
       values (18, now(), 'Work In Progress');

alter table listed_origins add column last_scheduled timestamptz;
comment on column listed_origins.last_scheduled is 'Time when this origin was scheduled to be visited last';

create index on listed_origins (last_scheduled);
