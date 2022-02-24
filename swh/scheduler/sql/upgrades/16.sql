insert into dbversion (version, release, description)
       values (16, now(), 'Work In Progress');

create table if not exists listers (
  id uuid primary key default uuid_generate_v4(),
  name text not null,
  instance_name text not null,
  created timestamptz not null default now(),  -- auto_now_add in the model
  current_state jsonb not null,
  updated timestamptz not null
);

comment on table listers is 'Lister instances known to the origin visit scheduler';
comment on column listers.name is 'Name of the lister (e.g. github, gitlab, debian, ...)';
comment on column listers.instance_name is 'Name of the current instance of this lister (e.g. framagit, bitbucket, ...)';
comment on column listers.created is 'Timestamp at which the lister was originally created';
comment on column listers.current_state is 'Known current state of this lister';
comment on column listers.updated is 'Timestamp at which the lister state was last updated';

-- lister schema
create unique index on listers (name, instance_name);

create table if not exists listed_origins (
  -- Basic information
  lister_id uuid not null references listers(id),
  url text not null,
  visit_type text not null,
  extra_loader_arguments jsonb not null,

  -- Whether this origin still exists or not
  enabled boolean not null,

  -- time-based information
  first_seen timestamptz not null default now(),
  last_seen timestamptz not null,

  -- potentially provided by the lister
  last_update timestamptz,

  primary key (lister_id, url, visit_type)
);

comment on table listed_origins is 'Origins known to the origin visit scheduler';
comment on column listed_origins.lister_id is 'Lister instance which owns this origin';
comment on column listed_origins.url is 'URL of the origin listed';
comment on column listed_origins.visit_type is 'Type of the visit which should be scheduled for the given url';
comment on column listed_origins.extra_loader_arguments is 'Extra arguments that should be passed to the loader for this origin';

comment on column listed_origins.enabled is 'Whether this origin has been seen during the last listing, and visits should be scheduled.';
comment on column listed_origins.first_seen is 'Time at which the origin was first seen by a lister';
comment on column listed_origins.last_seen is 'Time at which the origin was last seen by the lister';

comment on column listed_origins.last_update is 'Time of the last update to the origin recorded by the remote';
