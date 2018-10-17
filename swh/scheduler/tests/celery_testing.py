import os


def setup_celery():
    os.environ.setdefault('CELERY_BROKER_URL', 'memory://')
    os.environ.setdefault('CELERY_RESULT_BACKEND', 'cache+memory://')


class CeleryTestFixture:
    """Mix this in a test subject class to setup Celery config for testing
    purpose.

    Can be overriden by CELERY_BROKER_URL and CELERY_RESULT_BACKEND env vars.
    """

    def setUp(sel):
        setup_celery()
        super().setUp()
