--
-- PostgreSQL database dump
--

-- Dumped from database version 10.5 (Debian 10.5-1.pgdg+1)
-- Dumped by pg_dump version 10.5 (Debian 10.5-1.pgdg+1)

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
-- Name: task_policy; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_policy AS ENUM (
    'recurring',
    'oneshot'
);


--
-- Name: TYPE task_policy; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.task_policy IS 'Recurrence policy of the given task';


--
-- Name: task_priority; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_priority AS ENUM (
    'high',
    'normal',
    'low'
);


--
-- Name: TYPE task_priority; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.task_priority IS 'Priority of the given task';


--
-- Name: task_run_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_run_status AS ENUM (
    'scheduled',
    'started',
    'eventful',
    'uneventful',
    'failed',
    'permfailed',
    'lost'
);


--
-- Name: TYPE task_run_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.task_run_status IS 'Status of a given task run';


--
-- Name: task_status; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_status AS ENUM (
    'next_run_not_scheduled',
    'next_run_scheduled',
    'completed',
    'disabled'
);


--
-- Name: TYPE task_status; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TYPE public.task_status IS 'Status of a given task';


--
-- Name: task_record; Type: TYPE; Schema: public; Owner: -
--

CREATE TYPE public.task_record AS (
	task_id bigint,
	task_policy public.task_policy,
	task_status public.task_status,
	task_run_id bigint,
	arguments jsonb,
	type text,
	backend_id text,
	metadata jsonb,
	scheduled timestamp with time zone,
	started timestamp with time zone,
	ended timestamp with time zone,
	task_run_status public.task_run_status
);


SET default_tablespace = '';

SET default_with_oids = false;

--
-- Name: task; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task (
    id bigint NOT NULL,
    type text NOT NULL,
    arguments jsonb NOT NULL,
    next_run timestamp with time zone NOT NULL,
    current_interval interval,
    status public.task_status NOT NULL,
    policy public.task_policy DEFAULT 'recurring'::public.task_policy NOT NULL,
    retries_left bigint DEFAULT 0 NOT NULL,
    priority public.task_priority,
    CONSTRAINT task_check CHECK (((policy <> 'recurring'::public.task_policy) OR (current_interval IS NOT NULL)))
);


--
-- Name: TABLE task; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.task IS 'Schedule of recurring tasks';


--
-- Name: COLUMN task.arguments; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.arguments IS 'Arguments passed to the underlying job scheduler. Contains two keys, ''args'' (list) and ''kwargs'' (object).';


--
-- Name: COLUMN task.next_run; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.next_run IS 'The next run of this task should be run on or after that time';


--
-- Name: COLUMN task.current_interval; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.current_interval IS 'The interval between two runs of this task, taking into account the backoff factor';


--
-- Name: COLUMN task.policy; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.policy IS 'Whether the task is one-shot or recurring';


--
-- Name: COLUMN task.retries_left; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.retries_left IS 'The number of "short delay" retries of the task in case of transient failure';


--
-- Name: COLUMN task.priority; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task.priority IS 'Policy of the given task';


--
-- Name: swh_scheduler_create_tasks_from_temp(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_create_tasks_from_temp() RETURNS SETOF public.task
    LANGUAGE plpgsql
    AS $$
begin
  -- update the default values in one go
  -- this is separated from the insert/select to avoid too much
  -- juggling
  update tmp_task t
  set current_interval = tt.default_interval,
      retries_left = coalesce(retries_left, tt.num_retries, 0)
  from task_type tt
  where tt.type=t.type;

  insert into task (type, arguments, next_run, status, current_interval, policy,
                    retries_left, priority)
    select type, arguments, next_run, status, current_interval, policy,
           retries_left, priority
    from tmp_task t
    where not exists(select 1
                     from task
                     where type = t.type and
                           arguments->'args' = t.arguments->'args' and
                           arguments->'kwargs' = t.arguments->'kwargs' and
                           policy = t.policy and
                           priority is not distinct from t.priority and
                           status = t.status);

  return query
    select distinct t.*
    from tmp_task tt inner join task t on (
      tt.type = t.type and
      tt.arguments->'args' = t.arguments->'args' and
      tt.arguments->'kwargs' = t.arguments->'kwargs' and
      tt.policy = t.policy and
      tt.priority is not distinct from t.priority and
      tt.status = t.status
    );
end;
$$;


--
-- Name: FUNCTION swh_scheduler_create_tasks_from_temp(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_create_tasks_from_temp() IS 'Create tasks in bulk from the temporary table';


--
-- Name: swh_scheduler_delete_archived_tasks(bigint[], bigint[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_delete_archived_tasks(task_ids bigint[], task_run_ids bigint[]) RETURNS void
    LANGUAGE sql
    AS $$
  -- clean up task_run_ids
  delete from task_run where id in (select * from unnest(task_run_ids));
  -- clean up only tasks whose associated task_run are all cleaned up.
  -- Remaining tasks will stay there and will be cleaned up when
  -- remaining data have been indexed
  delete from task
  where id in (select t.id
               from task t left outer join task_run tr on t.id=tr.task
               where t.id in (select * from unnest(task_ids))
               and tr.task is null);
$$;


--
-- Name: FUNCTION swh_scheduler_delete_archived_tasks(task_ids bigint[], task_run_ids bigint[]); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_delete_archived_tasks(task_ids bigint[], task_run_ids bigint[]) IS 'Clean up archived tasks function';


--
-- Name: task_run; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_run (
    id bigint NOT NULL,
    task bigint NOT NULL,
    backend_id text,
    scheduled timestamp with time zone,
    started timestamp with time zone,
    ended timestamp with time zone,
    metadata jsonb,
    status public.task_run_status DEFAULT 'scheduled'::public.task_run_status NOT NULL
);


--
-- Name: TABLE task_run; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.task_run IS 'History of task runs sent to the job-running backend';


--
-- Name: COLUMN task_run.backend_id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_run.backend_id IS 'id of the task run in the job-running backend';


--
-- Name: COLUMN task_run.metadata; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_run.metadata IS 'Useful metadata for the given task run. For instance, the worker that took on the job, or the logs for the run.';


--
-- Name: swh_scheduler_end_task_run(text, public.task_run_status, jsonb, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_end_task_run(backend_id text, status public.task_run_status, metadata jsonb DEFAULT '{}'::jsonb, ts timestamp with time zone DEFAULT now()) RETURNS public.task_run
    LANGUAGE sql
    AS $$
  update task_run
    set ended = ts,
        status = swh_scheduler_end_task_run.status,
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_end_task_run.metadata
    where task_run.backend_id = swh_scheduler_end_task_run.backend_id
  returning *;
$$;


--
-- Name: swh_scheduler_grab_ready_tasks(text, timestamp with time zone, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_grab_ready_tasks(task_type text, ts timestamp with time zone DEFAULT now(), num_tasks bigint DEFAULT NULL::bigint, num_tasks_priority bigint DEFAULT NULL::bigint) RETURNS SETOF public.task
    LANGUAGE sql
    AS $$
  update task
    set status='next_run_scheduled'
    from (
        select id from swh_scheduler_peek_ready_tasks(task_type, ts, num_tasks, num_tasks_priority)
    ) next_tasks
    where task.id = next_tasks.id
  returning task.*;
$$;


--
-- Name: FUNCTION swh_scheduler_grab_ready_tasks(task_type text, ts timestamp with time zone, num_tasks bigint, num_tasks_priority bigint); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_grab_ready_tasks(task_type text, ts timestamp with time zone, num_tasks bigint, num_tasks_priority bigint) IS 'Grab tasks ready for scheduling and change their status';


--
-- Name: swh_scheduler_mktemp_task(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_mktemp_task() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_task (
    like task excluding indexes
  ) on commit drop;
  alter table tmp_task
    alter column retries_left drop not null,
    drop column id;
$$;


--
-- Name: FUNCTION swh_scheduler_mktemp_task(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_mktemp_task() IS 'Create a temporary table for bulk task creation';


--
-- Name: swh_scheduler_mktemp_task_run(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_mktemp_task_run() RETURNS void
    LANGUAGE sql
    AS $$
  create temporary table tmp_task_run (
    like task_run excluding indexes
  ) on commit drop;
  alter table tmp_task_run
    drop column id,
    drop column status;
$$;


--
-- Name: FUNCTION swh_scheduler_mktemp_task_run(); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_mktemp_task_run() IS 'Create a temporary table for bulk task run scheduling';


--
-- Name: swh_scheduler_nb_priority_tasks(bigint, public.task_priority); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_nb_priority_tasks(num_tasks_priority bigint, task_priority public.task_priority) RETURNS numeric
    LANGUAGE sql STABLE
    AS $$
  select ceil(num_tasks_priority * (select ratio from priority_ratio where id = task_priority)) :: numeric
$$;


--
-- Name: FUNCTION swh_scheduler_nb_priority_tasks(num_tasks_priority bigint, task_priority public.task_priority); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_nb_priority_tasks(num_tasks_priority bigint, task_priority public.task_priority) IS 'Given a priority task and a total number, compute the number of tasks to read';


--
-- Name: swh_scheduler_peek_no_priority_tasks(text, timestamp with time zone, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_peek_no_priority_tasks(task_type text, ts timestamp with time zone DEFAULT now(), num_tasks bigint DEFAULT NULL::bigint) RETURNS SETOF public.task
    LANGUAGE sql STABLE
    AS $$
select * from task
  where next_run <= ts
        and type = task_type
        and status = 'next_run_not_scheduled'
        and priority is null
  order by next_run
  limit num_tasks
  for update skip locked;
$$;


--
-- Name: FUNCTION swh_scheduler_peek_no_priority_tasks(task_type text, ts timestamp with time zone, num_tasks bigint); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_peek_no_priority_tasks(task_type text, ts timestamp with time zone, num_tasks bigint) IS 'Retrieve tasks without priority';


--
-- Name: swh_scheduler_peek_priority_tasks(text, timestamp with time zone, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_peek_priority_tasks(task_type text, ts timestamp with time zone DEFAULT now(), num_tasks_priority bigint DEFAULT NULL::bigint) RETURNS SETOF public.task
    LANGUAGE plpgsql
    AS $$
declare
  r record;
  count_row bigint;
  nb_diff bigint;
  nb_high bigint;
  nb_normal bigint;
  nb_low bigint;
begin
    -- expected values to fetch
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'high') into nb_high;
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'normal') into nb_normal;
    select swh_scheduler_nb_priority_tasks(num_tasks_priority, 'low') into nb_low;
    nb_diff := 0;
    count_row := 0;

    for r in select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_high, 'high')
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < nb_high then
        nb_normal := nb_normal + nb_high - count_row;
    end if;

    count_row := 0;
    for r in select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_normal, 'normal')
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < nb_normal then
        nb_low := nb_low + nb_normal - count_row;
    end if;

    return query select * from swh_scheduler_peek_tasks_with_priority(task_type, ts, nb_low, 'low');
end
$$;


--
-- Name: FUNCTION swh_scheduler_peek_priority_tasks(task_type text, ts timestamp with time zone, num_tasks_priority bigint); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_peek_priority_tasks(task_type text, ts timestamp with time zone, num_tasks_priority bigint) IS 'Retrieve priority tasks';


--
-- Name: swh_scheduler_peek_ready_tasks(text, timestamp with time zone, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_peek_ready_tasks(task_type text, ts timestamp with time zone DEFAULT now(), num_tasks bigint DEFAULT NULL::bigint, num_tasks_priority bigint DEFAULT NULL::bigint) RETURNS SETOF public.task
    LANGUAGE plpgsql
    AS $$
declare
  r record;
  count_row bigint;
  nb_diff bigint;
  nb_tasks bigint;
begin
    count_row := 0;

    for r in select * from swh_scheduler_peek_priority_tasks(task_type, ts, num_tasks_priority)
             order by priority, next_run
    loop
        count_row := count_row + 1;
        return next r;
    end loop;

    if count_row < num_tasks_priority then
       nb_tasks := num_tasks + num_tasks_priority - count_row;
    else
       nb_tasks := num_tasks;
    end if;

    for r in select * from swh_scheduler_peek_no_priority_tasks(task_type, ts, nb_tasks)
             order by priority, next_run
    loop
        return next r;
    end loop;

    return;
end
$$;


--
-- Name: FUNCTION swh_scheduler_peek_ready_tasks(task_type text, ts timestamp with time zone, num_tasks bigint, num_tasks_priority bigint); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_peek_ready_tasks(task_type text, ts timestamp with time zone, num_tasks bigint, num_tasks_priority bigint) IS 'Retrieve tasks with/without priority in order';


--
-- Name: swh_scheduler_peek_tasks_with_priority(text, timestamp with time zone, bigint, public.task_priority); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_peek_tasks_with_priority(task_type text, ts timestamp with time zone DEFAULT now(), num_tasks_priority bigint DEFAULT NULL::bigint, task_priority public.task_priority DEFAULT 'normal'::public.task_priority) RETURNS SETOF public.task
    LANGUAGE sql STABLE
    AS $$
  select *
  from task t
  where t.next_run <= ts
        and t.type = task_type
        and t.status = 'next_run_not_scheduled'
        and t.priority = task_priority
  order by t.next_run
  limit num_tasks_priority
  for update skip locked;
$$;


--
-- Name: FUNCTION swh_scheduler_peek_tasks_with_priority(task_type text, ts timestamp with time zone, num_tasks_priority bigint, task_priority public.task_priority); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_peek_tasks_with_priority(task_type text, ts timestamp with time zone, num_tasks_priority bigint, task_priority public.task_priority) IS 'Retrieve tasks with a given priority';


--
-- Name: swh_scheduler_schedule_task_run(bigint, text, jsonb, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_schedule_task_run(task_id bigint, backend_id text, metadata jsonb DEFAULT '{}'::jsonb, ts timestamp with time zone DEFAULT now()) RETURNS public.task_run
    LANGUAGE sql
    AS $$
  insert into task_run (task, backend_id, metadata, scheduled, status)
    values (task_id, backend_id, metadata, ts, 'scheduled')
  returning *;
$$;


--
-- Name: swh_scheduler_schedule_task_run_from_temp(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_schedule_task_run_from_temp() RETURNS void
    LANGUAGE plpgsql
    AS $$
begin
  insert into task_run (task, backend_id, metadata, scheduled, status)
    select task, backend_id, metadata, scheduled, 'scheduled'
      from tmp_task_run;
  return;
end;
$$;


--
-- Name: swh_scheduler_start_task_run(text, jsonb, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_start_task_run(backend_id text, metadata jsonb DEFAULT '{}'::jsonb, ts timestamp with time zone DEFAULT now()) RETURNS public.task_run
    LANGUAGE sql
    AS $$
  update task_run
    set started = ts,
        status = 'started',
        metadata = coalesce(task_run.metadata, '{}'::jsonb) || swh_scheduler_start_task_run.metadata
    where task_run.backend_id = swh_scheduler_start_task_run.backend_id
  returning *;
$$;


--
-- Name: swh_scheduler_task_to_archive(timestamp with time zone, timestamp with time zone, bigint, bigint); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_task_to_archive(ts_after timestamp with time zone, ts_before timestamp with time zone, last_id bigint DEFAULT '-1'::integer, lim bigint DEFAULT 10) RETURNS SETOF public.task_record
    LANGUAGE sql STABLE
    AS $$
   select t.id as task_id, t.policy as task_policy,
          t.status as task_status, tr.id as task_run_id,
          t.arguments, t.type, tr.backend_id, tr.metadata,
          tr.scheduled, tr.started, tr.ended, tr.status as task_run_status
   from task_run tr inner join task t on tr.task=t.id
   where ((t.policy = 'oneshot' and t.status in ('completed', 'disabled')) or
          (t.policy = 'recurring' and t.status = 'disabled')) and
          ((ts_after <= tr.started and tr.started < ts_before) or tr.started is null) and
          t.id > last_id
   order by tr.task, tr.started
   limit lim;
$$;


--
-- Name: FUNCTION swh_scheduler_task_to_archive(ts_after timestamp with time zone, ts_before timestamp with time zone, last_id bigint, lim bigint); Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON FUNCTION public.swh_scheduler_task_to_archive(ts_after timestamp with time zone, ts_before timestamp with time zone, last_id bigint, lim bigint) IS 'Read archivable tasks function';


--
-- Name: swh_scheduler_update_task_on_task_end(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.swh_scheduler_update_task_on_task_end() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
declare
  cur_task task%rowtype;
  cur_task_type task_type%rowtype;
  adjustment_factor float;
  new_interval interval;
begin
  select * from task where id = new.task into cur_task;
  select * from task_type where type = cur_task.type into cur_task_type;

  case
    when new.status = 'permfailed' then
      update task
        set status = 'disabled'
        where id = cur_task.id;
    when new.status in ('eventful', 'uneventful') then
      case
        when cur_task.policy = 'oneshot' then
          update task
            set status = 'completed'
            where id = cur_task.id;
        when cur_task.policy = 'recurring' then
          if new.status = 'uneventful' then
            adjustment_factor := 1/cur_task_type.backoff_factor;
          else
            adjustment_factor := 1/cur_task_type.backoff_factor;
          end if;
          new_interval := greatest(
            cur_task_type.min_interval,
            least(
              cur_task_type.max_interval,
              adjustment_factor * cur_task.current_interval));
          update task
            set status = 'next_run_not_scheduled',
                next_run = now() + new_interval,
                current_interval = new_interval,
                retries_left = coalesce(cur_task_type.num_retries, 0)
            where id = cur_task.id;
      end case;
    else -- new.status in 'failed', 'lost'
      if cur_task.retries_left > 0 then
        update task
          set status = 'next_run_not_scheduled',
              next_run = now() + coalesce(cur_task_type.retry_delay, interval '1 hour'),
              retries_left = cur_task.retries_left - 1
          where id = cur_task.id;
      else -- no retries left
        case
          when cur_task.policy = 'oneshot' then
            update task
              set status = 'disabled'
              where id = cur_task.id;
          when cur_task.policy = 'recurring' then
            update task
              set status = 'next_run_not_scheduled',
                  next_run = now() + cur_task.current_interval,
                  retries_left = coalesce(cur_task_type.num_retries, 0)
              where id = cur_task.id;
        end case;
      end if; -- retries
  end case;
  return null;
end;
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
-- Name: priority_ratio; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.priority_ratio (
    id public.task_priority NOT NULL,
    ratio double precision NOT NULL
);


--
-- Name: TABLE priority_ratio; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.priority_ratio IS 'Oneshot task''s reading ratio per priority';


--
-- Name: COLUMN priority_ratio.id; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.priority_ratio.id IS 'Task priority id';


--
-- Name: COLUMN priority_ratio.ratio; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.priority_ratio.ratio IS 'Percentage of tasks to read per priority';


--
-- Name: task_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_id_seq OWNED BY public.task.id;


--
-- Name: task_run_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.task_run_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: task_run_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.task_run_id_seq OWNED BY public.task_run.id;


--
-- Name: task_type; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.task_type (
    type text NOT NULL,
    description text NOT NULL,
    backend_name text NOT NULL,
    default_interval interval,
    min_interval interval,
    max_interval interval,
    backoff_factor double precision,
    max_queue_length bigint,
    num_retries bigint,
    retry_delay interval
);


--
-- Name: TABLE task_type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON TABLE public.task_type IS 'Types of schedulable tasks';


--
-- Name: COLUMN task_type.type; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.type IS 'Short identifier for the task type';


--
-- Name: COLUMN task_type.description; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.description IS 'Human-readable task description';


--
-- Name: COLUMN task_type.backend_name; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.backend_name IS 'Name of the task in the job-running backend';


--
-- Name: COLUMN task_type.default_interval; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.default_interval IS 'Default interval for newly scheduled tasks';


--
-- Name: COLUMN task_type.min_interval; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.min_interval IS 'Minimum interval between two runs of a task';


--
-- Name: COLUMN task_type.max_interval; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.max_interval IS 'Maximum interval between two runs of a task';


--
-- Name: COLUMN task_type.backoff_factor; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.backoff_factor IS 'Adjustment factor for the backoff between two task runs';


--
-- Name: COLUMN task_type.max_queue_length; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.max_queue_length IS 'Maximum length of the queue for this type of tasks';


--
-- Name: COLUMN task_type.num_retries; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.num_retries IS 'Default number of retries on transient failures';


--
-- Name: COLUMN task_type.retry_delay; Type: COMMENT; Schema: public; Owner: -
--

COMMENT ON COLUMN public.task_type.retry_delay IS 'Retry delay for the task';


--
-- Name: task id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task ALTER COLUMN id SET DEFAULT nextval('public.task_id_seq'::regclass);


--
-- Name: task_run id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_run ALTER COLUMN id SET DEFAULT nextval('public.task_run_id_seq'::regclass);


--
-- Data for Name: dbversion; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.dbversion (version, release, description) FROM stdin;
12	2018-10-10 13:29:22.932763+02	Work In Progress
\.


--
-- Data for Name: priority_ratio; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.priority_ratio (id, ratio) FROM stdin;
high	0.5
normal	0.299999999999999989
low	0.200000000000000011
\.


--
-- Data for Name: task; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.task (id, type, arguments, next_run, current_interval, status, policy, retries_left, priority) FROM stdin;
\.


--
-- Data for Name: task_run; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.task_run (id, task, backend_id, scheduled, started, ended, metadata, status) FROM stdin;
\.


--
-- Data for Name: task_type; Type: TABLE DATA; Schema: public; Owner: -
--

COPY public.task_type (type, description, backend_name, default_interval, min_interval, max_interval, backoff_factor, max_queue_length, num_retries, retry_delay) FROM stdin;
swh-loader-mount-dump-and-load-svn-repository	Loading svn repositories from svn dump	swh.loader.svn.tasks.MountAndLoadSvnRepository	1 day	1 day	1 day	1	1000	\N	\N
swh-deposit-archive-loading	Loading deposit archive into swh through swh-loader-tar	swh.deposit.loader.tasks.LoadDepositArchiveTsk	1 day	1 day	1 day	1	1000	3	\N
swh-deposit-archive-checks	Pre-checking deposit step before loading into swh archive	swh.deposit.loader.tasks.ChecksDepositTsk	1 day	1 day	1 day	1	1000	3	\N
swh-vault-cooking	Cook a Vault bundle	swh.vault.cooking_tasks.SWHCookingTask	1 day	1 day	1 day	1	10000	\N	\N
origin-load-hg	Loading mercurial repository swh-loader-mercurial	swh.loader.mercurial.tasks.LoadMercurialTsk	1 day	1 day	1 day	1	1000	\N	\N
origin-load-archive-hg	Loading archive mercurial repository swh-loader-mercurial	swh.loader.mercurial.tasks.LoadArchiveMercurialTsk	1 day	1 day	1 day	1	1000	\N	\N
origin-update-git	Update an origin of type git	swh.loader.git.tasks.UpdateGitRepository	64 days	12:00:00	64 days	2	5000	\N	\N
swh-lister-github-incremental	Incrementally list GitHub	swh.lister.github.tasks.IncrementalGitHubLister	1 day	1 day	1 day	1	\N	\N	\N
swh-lister-github-full	Full update of GitHub repos list	swh.lister.github.tasks.FullGitHubRelister	90 days	90 days	90 days	1	\N	\N	\N
swh-lister-debian	List a Debian distribution	swh.lister.debian.tasks.DebianListerTask	1 day	1 day	1 day	1	\N	\N	\N
swh-lister-gitlab-incremental	Incrementally list a Gitlab instance	swh.lister.gitlab.tasks.IncrementalGitLabLister	1 day	1 day	1 day	1	\N	\N	\N
swh-lister-gitlab-full	Full update of a Gitlab instance's repos list	swh.lister.gitlab.tasks.FullGitLabRelister	90 days	90 days	90 days	1	\N	\N	\N
swh-lister-pypi	Full pypi lister	swh.lister.pypi.tasks.PyPIListerTask	1 day	1 day	1 day	1	\N	\N	\N
origin-update-pypi	Load Pypi origin	swh.loader.pypi.tasks.LoadPyPI	64 days	12:00:00	64 days	2	5000	\N	\N
\.


--
-- Name: task_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.task_id_seq', 1, false);


--
-- Name: task_run_id_seq; Type: SEQUENCE SET; Schema: public; Owner: -
--

SELECT pg_catalog.setval('public.task_run_id_seq', 1, false);


--
-- Name: dbversion dbversion_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.dbversion
    ADD CONSTRAINT dbversion_pkey PRIMARY KEY (version);


--
-- Name: priority_ratio priority_ratio_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.priority_ratio
    ADD CONSTRAINT priority_ratio_pkey PRIMARY KEY (id);


--
-- Name: task task_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT task_pkey PRIMARY KEY (id);


--
-- Name: task_run task_run_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_run
    ADD CONSTRAINT task_run_pkey PRIMARY KEY (id);


--
-- Name: task_type task_type_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_type
    ADD CONSTRAINT task_type_pkey PRIMARY KEY (type);


--
-- Name: task_args; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_args ON public.task USING btree (((arguments -> 'args'::text)));


--
-- Name: task_kwargs; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_kwargs ON public.task USING gin (((arguments -> 'kwargs'::text)));


--
-- Name: task_next_run_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_next_run_idx ON public.task USING btree (next_run);


--
-- Name: task_priority_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_priority_idx ON public.task USING btree (priority);


--
-- Name: task_run_backend_id_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_run_backend_id_idx ON public.task_run USING btree (backend_id);


--
-- Name: task_run_id_asc_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_run_id_asc_idx ON public.task_run USING btree (task, started);


--
-- Name: task_run_task_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_run_task_idx ON public.task_run USING btree (task);


--
-- Name: task_type_idx; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX task_type_idx ON public.task USING btree (type);


--
-- Name: task_run update_task_on_task_end; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER update_task_on_task_end AFTER UPDATE OF status ON public.task_run FOR EACH ROW WHEN ((new.status <> ALL (ARRAY['scheduled'::public.task_run_status, 'started'::public.task_run_status]))) EXECUTE PROCEDURE public.swh_scheduler_update_task_on_task_end();


--
-- Name: task task_priority_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT task_priority_fkey FOREIGN KEY (priority) REFERENCES public.priority_ratio(id);


--
-- Name: task_run task_run_task_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task_run
    ADD CONSTRAINT task_run_task_fkey FOREIGN KEY (task) REFERENCES public.task(id);


--
-- Name: task task_type_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.task
    ADD CONSTRAINT task_type_fkey FOREIGN KEY (type) REFERENCES public.task_type(type);


--
-- PostgreSQL database dump complete
--

