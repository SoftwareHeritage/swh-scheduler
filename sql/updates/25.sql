insert into dbversion (version, release, description)
       values (25, now(), 'Work In Progress');

create table scheduler_metrics (
  lister_id uuid not null references listers(id),
  visit_type text not null,
  last_update timestamptz not null,
  origins_known int not null default 0,
  origins_enabled int not null default 0,
  origins_never_visited int not null default 0,
  origins_with_pending_changes int not null default 0,

  primary key (lister_id, visit_type)
);

comment on table scheduler_metrics is 'Cache of per-lister metrics for the scheduler, collated between the listed_origins and origin_visit_stats tables.';
comment on column scheduler_metrics.lister_id is 'Lister instance on which metrics have been aggregated';
comment on column scheduler_metrics.visit_type is 'Visit type on which metrics have been aggregated';
comment on column scheduler_metrics.last_update is 'Last update of these metrics';
comment on column scheduler_metrics.origins_known is 'Number of known (enabled or disabled) origins';
comment on column scheduler_metrics.origins_enabled is 'Number of origins that were present in the latest listing';
comment on column scheduler_metrics.origins_never_visited is 'Number of origins that have never been successfully visited';
comment on column scheduler_metrics.origins_with_pending_changes is 'Number of enabled origins with known activity since our last visit';


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
        enabled and lo.last_update > greatest(ovs.last_eventful, ovs.last_uneventful)
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
