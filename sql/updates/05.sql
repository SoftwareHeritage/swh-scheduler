-- SWH Scheduler Schema upgrade
-- from_version: 04
-- to_version: 05
-- description: Add reccurrence logic for temporary failures and one-shot tasks

alter type task_status add value if not exists 'completed' before 'disabled';
alter type task_run_status add value if not exists 'permfailed' after 'failed';

begin;

insert into dbversion (version, release, description)
values (5, now(), 'Work In Progress');

alter table task_type add column num_retries bigint;
alter table task_type add column retry_delay interval;

comment on column task_type.num_retries is 'Default number of retries on transient failures';
comment on column task_type.retry_delay is 'Retry delay for the task';

create type task_policy as enum ('recurring', 'oneshot');
comment on type task_policy is 'Recurrence policy of the given task';

alter table task add column policy task_policy not null default 'recurring';
alter table task add column retries_left bigint not null default 0;

comment on column task.policy is 'Whether the task is one-shot or recurring';
comment on column task.retries_left is 'The number of "short delay" retries of the task in case of '
                                       'transient failure';


create or replace function swh_scheduler_mktemp_task ()
  returns void
  language sql
as $$
  create temporary table tmp_task (
    like task excluding indexes
  ) on commit drop;
  alter table tmp_task
    drop column id,
    drop column current_interval,
    drop column status,
    alter column policy drop not null,
    alter column retries_left drop not null;
$$;

comment on function swh_scheduler_mktemp_task () is 'Create a temporary table for bulk task creation';

create or replace function swh_scheduler_create_tasks_from_temp ()
  returns setof task
  language plpgsql
as $$
begin
  return query
  insert into task (type, arguments, next_run, status, current_interval, policy, retries_left)
    select type, arguments, next_run, 'next_run_not_scheduled',
           (select default_interval from task_type tt where tt.type = tmp_task.type),
           coalesce(policy, 'recurring'),
           coalesce(retries_left, (select num_retries from task_type tt where tt.type = tmp_task.type), 0)
      from tmp_task
  returning task.*;
end;
$$;

comment on function swh_scheduler_create_tasks_from_temp () is 'Create tasks in bulk from the temporary table';

drop trigger update_interval_on_task_end on task_run;
drop function swh_scheduler_compute_new_task_interval (text, interval, task_run_status) cascade;
drop function swh_scheduler_update_task_interval () cascade;

create or replace function swh_scheduler_update_task_on_task_end ()
  returns trigger
  language plpgsql
as $$
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
              next_run = now() + cur_task_type.retry_delay,
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

create trigger update_task_on_task_end
  after update of status on task_run
  for each row
  when (new.status NOT IN ('scheduled', 'started'))
  execute procedure swh_scheduler_update_task_on_task_end ();

commit;
