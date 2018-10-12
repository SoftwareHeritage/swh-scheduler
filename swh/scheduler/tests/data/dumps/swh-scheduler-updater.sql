--
-- PostgreSQL database dump
--

-- Dumped from database version 10.4 (Debian 10.4-2.pgdg+1)
-- Dumped by pg_dump version 10.4 (Debian 10.4-2.pgdg+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: plpgsql; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS plpgsql WITH SCHEMA pg_catalog;


--
-- Name: EXTENSION plpgsql; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION plpgsql IS 'PL/pgSQL procedural language';


--
-- Name: btree_gist; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS btree_gist WITH SCHEMA public;


--
-- Name: EXTENSION btree_gist; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION btree_gist IS 'support for indexing common datatypes in GiST';


--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: -
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


--
-- Name: origin_type; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.origin_type AS ENUM (
    'git',
    'svn',
    'hg',
    'deb'
);


--
-- Name: TYPE origin_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.origin_type IS 'Url''s repository type';


--
-- Name: sha1; Type: DOMAIN; Schema: public; Owner: -
--

CREATE DOMAIN public.sha1 AS bytea
	CONSTRAINT sha1_check CHECK ((length(VALUE) = 20));


--
-- Name: hash_sha1(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.hash_sha1(text) RETURNS public.sha1
    LANGUAGE sql IMMUTABLE STRICT
    AS $_$
   select public.digest($1, 'sha1') :: sha1
$_$;


--
-- Name: FUNCTION hash_sha1(text); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.hash_sha1(text) IS 'Compute sha1 hash as text';


--
-- Name: swh_cache_put(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_cache_put() RETURNS void
    LANGUAGE plpgsql
    AS $$
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


--
-- Name: FUNCTION swh_cache_put(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_cache_put() IS 'Write to cache temporary events';


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: cache; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.cache (
    id public.sha1 NOT NULL,
    url text NOT NULL,
    origin_type public.origin_type NOT NULL,
    cnt integer DEFAULT 1,
    first_seen timestamp with time zone DEFAULT now() NOT NULL,
    last_seen timestamp with time zone NOT NULL
);


--
-- Name: swh_cache_read(timestamp with time zone, integer); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_cache_read(ts timestamp with time zone, lim integer) RETURNS SETOF public.cache
    LANGUAGE sql STABLE
    AS $$
  select id, url, origin_type, cnt, first_seen, last_seen
  from cache
  where last_seen <= ts
  limit lim;
$$;


--
-- Name: FUNCTION swh_cache_read(ts timestamp with time zone, lim integer); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_cache_read(ts timestamp with time zone, lim integer) IS 'Read cache entries';


--
-- Name: swh_mktemp_cache(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_mktemp_cache() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_cache (
    like cache including defaults
  ) on commit drop;
  alter table tmp_cache drop column id;
$$;


--
-- Name: dbversion; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.dbversion (
    version integer NOT NULL,
    release timestamp with time zone NOT NULL,
    description text NOT NULL
);


--
-- Name: TABLE dbversion; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.dbversion IS 'Schema update tracking';


--
-- Data for Name: cache; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.cache (id, url, origin_type, cnt, first_seen, last_seen) FROM stdin;
\.


--
-- Data for Name: dbversion; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.dbversion (version, release, description) FROM stdin;
1	2018-06-05 13:57:29.282695+02	Work In Progress
\.


--
-- Name: cache cache_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.cache
    ADD CONSTRAINT cache_pkey PRIMARY KEY (id);


--
-- Name: dbversion dbversion_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbversion
    ADD CONSTRAINT dbversion_pkey PRIMARY KEY (version);


--
-- Name: cache_last_seen_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cache_last_seen_idx ON public.cache USING btree (last_seen);


--
-- Name: cache_url_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX cache_url_idx ON public.cache USING btree (url);


--
-- PostgreSQL database dump complete
--

