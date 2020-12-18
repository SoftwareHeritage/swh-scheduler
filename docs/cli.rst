.. _swh-scheduler-cli:

Command-line interface
======================

Shared command-line interface
-----------------------------

.. click:: swh.scheduler.cli:cli
  :prog: swh scheduler
  :nested: short

Scheduler task utilities
------------------------

.. click:: swh.scheduler.cli.task:task
  :prog: swh scheduler task
  :nested: full

.. click:: swh.scheduler.cli.task_type:task_type
  :prog: swh scheduler task_type
  :nested: full


Scheduler server utilities
--------------------------

.. click:: swh.scheduler.cli.admin:runner
  :prog: swh scheduler runner
  :nested: full

.. click:: swh.scheduler.cli.admin:listener
  :prog: swh scheduler listener
  :nested: full

.. click:: swh.scheduler.cli.admin:rpc_server
  :prog: swh scheduler rpc-serve
  :nested: full

.. click:: swh.scheduler.cli.celery_monitor:celery_monitor
  :prog: swh scheduler celery-monitor
  :nested: full
