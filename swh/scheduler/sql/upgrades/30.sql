-- SWH DB schema upgrade
-- from_version: 29
-- to_version: 30
-- description: merge a bunch of fields in origin_visit_stats

insert into dbversion (version, release, description)
       values (30, now(), 'Work In Progress');


create type last_visit_status as enum ('successful', 'failed', 'not_found');
comment on type last_visit_status is 'Record of the status of the last visit of an origin';

alter table origin_visit_stats
  add column last_successful timestamptz,
  add column last_visit timestamptz,
  add column last_visit_status last_visit_status;

comment on column origin_visit_stats.last_successful is 'Date of the last successful visit, at which we recorded the `last_snapshot`';
comment on column origin_visit_stats.last_visit is 'Date of the last visit overall';
comment on column origin_visit_stats.last_visit_status is 'Status of the last visit';

update origin_visit_stats
  set last_successful = greatest(last_eventful, last_uneventful),
      last_visit = greatest(last_eventful, last_uneventful, last_failed, last_notfound);

update origin_visit_stats
  set last_visit_status =
    case
      when last_visit = last_failed then 'failed'::last_visit_status
      when last_visit = last_notfound then 'not_found'::last_visit_status
      else 'successful'::last_visit_status
    end
  where last_visit is not null;


create or replace function update_metrics(lister_id uuid default NULL, ts timestamptz default now())
  returns setof scheduler_metrics
  language sql
as $$
  insert into scheduler_metrics (
    lister_id, visit_type, last_update,
    origins_known, origins_enabled,
    origins_never_visited, origins_with_pending_changes
  )
    select
      lo.lister_id, lo.visit_type, coalesce(ts, now()) as last_update,
      count(*) as origins_known,
      count(*) filter (where enabled) as origins_enabled,
      count(*) filter (where
        enabled and last_snapshot is NULL
      ) as origins_never_visited,
      count(*) filter (where
        enabled and lo.last_update > last_successful
      ) as origins_with_pending_changes
    from listed_origins lo
    left join origin_visit_stats ovs using (url, visit_type)
    where
      -- update only for the requested lister
      update_metrics.lister_id = lo.lister_id
      -- or for all listers if the function argument is null
      or update_metrics.lister_id is null
    group by (lister_id, visit_type)
  on conflict (lister_id, visit_type) do update
    set
      last_update = EXCLUDED.last_update,
      origins_known = EXCLUDED.origins_known,
      origins_enabled = EXCLUDED.origins_enabled,
      origins_never_visited = EXCLUDED.origins_never_visited,
      origins_with_pending_changes = EXCLUDED.origins_with_pending_changes
  returning *
$$;

comment on function update_metrics(uuid, timestamptz) is 'Update metrics for the given lister_id';

alter table origin_visit_stats
  drop column last_eventful,
  drop column last_uneventful,
  drop column last_failed,
  drop column last_notfound;
