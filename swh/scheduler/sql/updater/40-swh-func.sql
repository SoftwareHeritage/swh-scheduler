-- Postgresql index helper function
create or replace function hash_sha1(text)
       returns sha1
as $$
   select public.digest($1, 'sha1') :: sha1
$$ language sql strict immutable;

comment on function hash_sha1(text) is 'Compute sha1 hash as text';

-- create a temporary table for cache tmp_cache,
create or replace function swh_mktemp_cache()
    returns void
    language sql
as $$
  create temporary table tmp_cache (
    like cache including defaults
  ) on commit drop;
  alter table tmp_cache drop column id;
$$;

create or replace function swh_cache_put()
    returns void
    language plpgsql
as $$
begin
    insert into cache (id, url, origin_type, cnt, last_seen)
    select hash_sha1(url), url, origin_type, cnt, last_seen
    from tmp_cache t
    on conflict(id)
    do update set cnt = (select cnt from cache where id=excluded.id) + excluded.cnt,
                  last_seen = excluded.last_seen;
    return;
end
$$;

comment on function swh_cache_put() is 'Write to cache temporary events';

create or replace function swh_cache_read(ts timestamptz, lim integer)
    returns setof cache
    language sql stable
as $$
  select id, url, origin_type, cnt, first_seen, last_seen
  from cache
  where last_seen <= ts
  limit lim;
$$;

comment on function swh_cache_read(timestamptz, integer) is 'Read cache entries';
