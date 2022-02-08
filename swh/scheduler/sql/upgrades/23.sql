insert into dbversion (version, release, description)
       values (23, now(), 'Work In Progress');

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
                next_run = new.ended + new_interval,
                current_interval = new_interval,
                retries_left = coalesce(cur_task_type.num_retries, 0)
            where id = cur_task.id;
      end case;
    else -- new.status in 'failed', 'lost'
      if cur_task.retries_left > 0 then
        update task
          set status = 'next_run_not_scheduled',
              next_run = new.ended + coalesce(cur_task_type.retry_delay, interval '1 hour'),
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
                  next_run = new.ended + cur_task.current_interval,
                  retries_left = coalesce(cur_task_type.num_retries, 0)
              where id = cur_task.id;
        end case;
      end if; -- retries
  end case;
  return null;
end;
$$;

