swh-scheduler
=============

Job scheduler for the Software Heritage project.

Task manager for asynchronous/delayed tasks, used for both recurrent (e.g.,
listing a forge, loading new stuff from a Git repository) and one-off
activities (e.g., loading a specific version of a source package).


# Tests

## Running test manually

### Test data

To be able to run (unit) tests, you need to have the
[[https://forge.softwareheritage.org/source/swh-storage-testdata.git|swh-storage-testdata]]
in the parent directory. If you have set your environment following the
[[ https://docs.softwareheritage.org/devel/getting-started.html#getting-started|Getting started]]
document everythong should be set up just fine.

Otherwise:

```
~/.../swh-scheduler$ git clone https://forge.softwareheritage.org/source/swh-storage-testdata.git ../swh-storage-testdata
```

### Required services

Unit tests may require a running celery broker on your system (rabbitmq by
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
