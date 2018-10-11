import os
import urllib

from celery import current_app


CELERY_BROKER_PROTOCOLS = ['amqp', 'redis', 'sqs']


def setup_celery():
    broker_url = None
    if 'BROKER_URL' in os.environ:
        broker_url = os.environ['BROKER_URL']

    elif 'PIFPAF_URLS' in os.environ:
        urls = os.environ['PIFPAF_URLS'].split(';')
        urls = [x.replace('rabbit://', 'amqp://') for x in urls]
        for url in urls:
            scheme = urllib.parse.splittype(url)[0]
            if scheme in CELERY_BROKER_PROTOCOLS:
                broker_url = url
                break
    if broker_url:
        current_app.conf.broker_url = broker_url


class CeleryTestFixture:
    """Mix this in a test subject class to get Celery testing support configured
    via environment variables, typically set up by pifpaf.

    It expect a connection url to a celery broker either as a BROKER_URL or a
    url listed in a PIFPAF_URL environment variable.
    """

    def setUp(sel):
        setup_celery()
        super().setUp()
