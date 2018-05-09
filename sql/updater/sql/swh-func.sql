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
    insert into cache (id, url, last_seen)
    select hash_sha1(url), url, last_seen
    from tmp_cache t
    on conflict(id)
    do update set rate = (select rate from cache where id=excluded.id) + 1,
                  last_seen = excluded.last_seen;
    return;
end
$$;
