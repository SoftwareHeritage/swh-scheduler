Software Heritage Scheduler Simulator
=====================================

This component simulates the interaction between the scheduling and loading
infrastructure of Software Heritage. This allows quick(er) development of new
task scheduling policies without having to wait for the actual infrastructure
to perform (heavy) loading tasks.

Simulator components
--------------------

- real instance of the scheduler database
- simulated task queues
- simulated workers
- simulated load tasks
- simulated archive -> scheduler feedback loop

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
   python3 -m swh.scheduler.simulator fill_test_data

   # Run the simulator itself, interacting with the scheduler database you've
   # just seeded.
   python3 -m swh.scheduler.simulator run