-- SWH DB schema upgrade
-- from_version: 36
-- to_version: 37
-- description: add a couple of new fields to the listed_origins table


alter table listed_origins add column is_fork boolean, add column forked_from_url text;

comment on column listed_origins.is_fork is 'Whether the origin is identified as a fork, if available';
comment on column listed_origins.forked_from_url is 'URL of the origin this origin is forked from, if available';
