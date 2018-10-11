swh-scheduler
=============

Job scheduler for the Software Heritage project.

Task manager for asynchronous/delayed tasks, used for both recurrent (e.g.,
listing a forge, loading new stuff from a Git repository) and one-off
activities (e.g., loading a specific version of a source package).


# Tests

Unit tests may require a running celery broker on your system (rabitmq by
default). You can set the `BROKER_URL` environment variable to specify the url
to be used.

If you do not want to use your system's broker (or do not want to have one
running), you shold use [[ https://github.com/jd/pifpaf | pifpaf ]] to take
care of that for you.


For example:

```
$ pifpaf --env-prefix PG run postgresql -- \
  pifpaf --env-prefix AMQP run rabbitmq nosetests

.....................................
----------------------------------------------------------------------
Ran 37 tests in 15.578s

OK
```
