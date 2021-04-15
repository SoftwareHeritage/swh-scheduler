.. _swh-scheduler-simulator:

Software Heritage Scheduler Simulator
=====================================

This component simulates the interaction between the scheduling and loading
infrastructure of Software Heritage. This allows quick(er) development of new
task scheduling policies without having to wait for the actual infrastructure
to perform (heavy) loading tasks.

Simulator components
--------------------

- real instance of the scheduler database
- simulated task queues: replaces RabbitMQ with simple in-memory structures
- simulated workers: replaces Celery with simple while loops
- simulated load tasks: replaces loaders with noops that take a certain time,
  and generate synthetic OriginVisitStatus objects
- simulated archive -> scheduler feedback loop: OriginVisitStatus objects are
  pushed to a simple queue which gets processed by the scheduler journal
  client's process function directly (instead of going through swh.storage and
  swh.journal (kafka))

In short, only the scheduler database and scheduler logic is kept; every other
component (RabbitMQ, Celery, Kafka, SWH loaders, SWH storage) is either replaced
with an barebones in-process utility, or removed entirely.

Installing the simulator
------------------------

The simulator depends on SimPy and other specific libraries. To install them,
please use:

.. code-block:: bash

   pip install 'swh.scheduler[simulator]'

Running the simulator
---------------------

The simulator uses a real instance of the scheduler database, which is (at
least for now) persistent across runs of the simulator. You need to set that up
beforehand:

.. code-block:: bash

   # if you want to use a temporary instance of postgresql
   eval `pifpaf run postgresql`

   # Set this variable for the simulator to know which db to connect to. pifpaf
   # sets other variables like PGPORT, PGHOST, ...
   export PGDATABASE=swh-scheduler

   # Create/initialize the scheduler database
   swh db create scheduler -d $PGDATABASE
   swh db init scheduler -d $PGDATABASE

   # This generates some data in the scheduler database. You can also feed the
   # database with more realistic data, e.g. from a lister or from a dump of the
   # production database.
   swh scheduler -d "dbname=$PGDATABASE" simulator fill-test-data

   # Run the simulator itself, interacting with the scheduler database you've
   # just seeded.
   swh scheduler -d "dbname=$PGDATABASE" simulator run --scheduler origin_scheduler


Origin model
------------

The origin model is how we represent the behaviors of origins: when they are
created/discovered, how many commits they get and when, and when they fail to load.

For now it is only a simple approximation designed to exercise simple cases:
origin creation/discovery, a continuous stream of commits, and failure if they have
too many commits to load at once.
For details, see :py:mod:`swh.scheduler.simulator.origins`.

To keep the simulation fast enough, each origin's state is kept in memory, so the
simulator process will linearly increase in memory usage as it runs.
