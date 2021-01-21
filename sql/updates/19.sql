insert into dbversion (version, release, description)
       values (19, now(), 'Work In Progress');

create table origin_visit_stats (
  url text not null,
  visit_type text not null,
  last_eventful timestamptz,
  last_uneventful timestamptz,
  last_failed timestamptz,
  last_notfound timestamptz,
  last_snapshot bytea,

  primary key (url, visit_type)
);

comment on column origin_visit_stats.url is 'Origin URL';
comment on column origin_visit_stats.visit_type is 'Type of the visit for the given url';
comment on column origin_visit_stats.last_eventful is 'Date of the last eventful event';
comment on column origin_visit_stats.last_uneventful is 'Date of the last uneventful event';
comment on column origin_visit_stats.last_failed is 'Date of the last failed event';
comment on column origin_visit_stats.last_notfound is 'Date of the last notfound event';
comment on column origin_visit_stats.last_snapshot is 'sha1_git of the last visit snapshot';
