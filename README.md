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
document everything should be set up just fine.

Otherwise:

```
~/.../swh-scheduler$ git clone https://forge.softwareheritage.org/source/swh-storage-testdata.git ../swh-storage-testdata
```

### Required services

Unit tests that require a running celery broker uses an in memory broker/result
backend by default, but you can choose to use a true broker by setting
`CELERY_BROKER_URL` and `CELERY_RESULT_BACKEND` environment variables up.

For example:

```
$ CELERY_BROKER_URL=amqp://localhost pifpaf run postgresql nosetests

.....................................
----------------------------------------------------------------------
Ran 37 tests in 15.578s

OK
```
