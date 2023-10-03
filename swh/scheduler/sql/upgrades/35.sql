-- SWH DB schema upgrade
-- from_version: 34
-- to_version: 35
-- description: Set default max_queue_length to 1000

insert into dbversion (version, release, description)
       values (35, now(), 'Work In Progress');

-- Because the register task type routine does not provide the value, it's left
-- unchecked. Once a new lister starts listing origins, the scheduler keeps on
-- scheduling new origins in the queue without limits. As the consumption may be slower
-- than the production, that tends towards too much resources usage in rabbitmq.
alter table task_type alter column max_queue_length set default 1000;
