-- SWH DB schema upgrade
-- from_version: 31
-- to_version: 32
-- description: Use a temporary table to update scheduler_metrics

insert into dbversion (version, release, description)
       values (32, now(), 'Work In Progress');

create or replace function update_metrics(lister_id uuid default NULL, ts timestamptz default now())
  returns setof scheduler_metrics
  language plpgsql
as $$
  begin
    -- If we do the following select as a subquery in the insert statement below,
    -- PostgreSQL prevents the use of parallel queries. So we do the select into a
    -- temporary table, which doesn't suffer this limitation.

    create temporary table tmp_update_metrics
    on commit drop
    as select
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
    group by (lo.lister_id, lo.visit_type);

    return query
      insert into scheduler_metrics (
        lister_id, visit_type, last_update,
        origins_known, origins_enabled,
        origins_never_visited, origins_with_pending_changes
      )
      select * from tmp_update_metrics
      on conflict on constraint scheduler_metrics_pkey do update
        set
          last_update = EXCLUDED.last_update,
          origins_known = EXCLUDED.origins_known,
          origins_enabled = EXCLUDED.origins_enabled,
          origins_never_visited = EXCLUDED.origins_never_visited,
          origins_with_pending_changes = EXCLUDED.origins_with_pending_changes
      returning *;
  end;
$$;

comment on function update_metrics(uuid, timestamptz) is 'Update metrics for the given lister_id';
