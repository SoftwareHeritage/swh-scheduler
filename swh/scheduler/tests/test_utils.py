# Copyright (C) 2017-2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import unittest

from datetime import timezone
from nose.tools import istest
from unittest.mock import patch

from swh.scheduler import utils


class UtilsTest(unittest.TestCase):

    @istest
    @patch('swh.scheduler.utils.datetime')
    def create_oneshot_task_dict_simple(self, mock_datetime):
        mock_datetime.now.return_value = 'some-date'

        actual_task = utils.create_oneshot_task_dict('some-task-type')

        expected_task = {
            'policy': 'oneshot',
            'type': 'some-task-type',
            'next_run': 'some-date',
            'arguments': {
                'args': [],
                'kwargs': {},
            },
            'priority': None,
        }

        self.assertEqual(actual_task, expected_task)
        mock_datetime.now.assert_called_once_with(tz=timezone.utc)

    @istest
    @patch('swh.scheduler.utils.datetime')
    def create_oneshot_task_dict_other_call(self, mock_datetime):
        mock_datetime.now.return_value = 'some-other-date'

        actual_task = utils.create_oneshot_task_dict(
            'some-task-type', 'arg0', 'arg1',
            priority='high', other_stuff='normal'
        )

        expected_task = {
            'policy': 'oneshot',
            'type': 'some-task-type',
            'next_run': 'some-other-date',
            'arguments': {
                'args': ('arg0', 'arg1'),
                'kwargs': {'other_stuff': 'normal'},
            },
            'priority': 'high',
        }

        self.assertEqual(actual_task, expected_task)
        mock_datetime.now.assert_called_once_with(tz=timezone.utc)
