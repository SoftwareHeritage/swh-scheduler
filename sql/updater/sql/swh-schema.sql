create table dbversion
(
  version     int primary key,
  release     timestamptz not null,
  description text not null
);

comment on table dbversion is 'Schema update tracking';

-- a SHA1 checksum (not necessarily originating from Git)
create domain sha1 as bytea check (length(value) = 20);

insert into dbversion (version, release, description)
       values (1, now(), 'Work In Progress');

create table cache (
   id sha1 primary key,
   url text not null,
   rate int default 1,
   last_seen timestamptz not null
);

create index on cache(url);
create index on cache(last_seen);
