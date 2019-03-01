# Copyright (C) 2019  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information

import datetime
import re
import tempfile
from unittest.mock import patch

from click.testing import CliRunner
import pytest

from swh.scheduler.cli import cli
from swh.scheduler.utils import create_task_dict


CLI_CONFIG = '''
scheduler:
    cls: foo
    args: {}
'''


def invoke(scheduler, catch_exceptions, args):
    runner = CliRunner()
    with patch('swh.scheduler.cli.get_scheduler') as get_scheduler_mock, \
            tempfile.NamedTemporaryFile('a', suffix='.yml') as config_fd:
        config_fd.write(CLI_CONFIG)
        config_fd.seek(0)
        get_scheduler_mock.return_value = scheduler
        result = runner.invoke(cli, ['-C' + config_fd.name] + args)
    if not catch_exceptions and result.exception:
        print(result.output)
        raise result.exception
    return result


def test_schedule_tasks(swh_scheduler):
    csv_data = (
        b'swh-test-ping;[["arg1", "arg2"]];{"key": "value"};'
        + datetime.datetime.utcnow().isoformat().encode() + b'\n'
        + b'swh-test-ping;[["arg3", "arg4"]];{"key": "value"};'
        + datetime.datetime.utcnow().isoformat().encode() + b'\n')
    with tempfile.NamedTemporaryFile(suffix='.csv') as csv_fd:
        csv_fd.write(csv_data)
        csv_fd.seek(0)
        result = invoke(swh_scheduler, False, [
            'task', 'schedule',
            '-d', ';',
            csv_fd.name
        ])
    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Created 2 tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: recurring
  Args:
    \['arg1', 'arg2'\]
  Keyword args:
    key: 'value'

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: recurring
  Args:
    \['arg3', 'arg4'\]
  Keyword args:
    key: 'value'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_schedule_tasks_columns(swh_scheduler):
    with tempfile.NamedTemporaryFile(suffix='.csv') as csv_fd:
        csv_fd.write(
            b'swh-test-ping;oneshot;["arg1", "arg2"];{"key": "value"}\n')
        csv_fd.seek(0)
        result = invoke(swh_scheduler, False, [
            'task', 'schedule',
            '-c', 'type', '-c', 'policy', '-c', 'args', '-c', 'kwargs',
            '-d', ';',
            csv_fd.name
        ])
    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Created 1 tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
    'arg1'
    'arg2'
  Keyword args:
    key: 'value'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_schedule_task(swh_scheduler):
    result = invoke(swh_scheduler, False, [
        'task', 'add',
        'swh-test-ping', 'arg1', 'arg2', 'key=value',
    ])
    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Created 1 tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: recurring
  Args:
    'arg1'
    'arg2'
  Keyword args:
    key: 'value'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_pending_tasks_none(swh_scheduler):
    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 0 swh-test-ping tasks

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_pending_tasks(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task2['next_run'] += datetime.timedelta(days=1)
    swh_scheduler.create_tasks([task1, task2])

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 swh-test-ping tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value1'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output

    swh_scheduler.grab_ready_tasks('swh-test-ping')

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 0 swh-test-ping tasks

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_pending_tasks_filter(swh_scheduler):
    task = create_task_dict('swh-test-multiping', 'oneshot', key='value')
    swh_scheduler.create_tasks([task])

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 0 swh-test-ping tasks

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_pending_tasks_filter_2(swh_scheduler):
    swh_scheduler.create_tasks([
        create_task_dict('swh-test-multiping', 'oneshot', key='value'),
        create_task_dict('swh-test-ping', 'oneshot', key='value2'),
    ])

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 swh-test-ping tasks

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


# Fails because "task list-pending --limit 3" only returns 2 tasks, because
# of how compute_nb_tasks_from works.
@pytest.mark.xfail
def test_list_pending_tasks_limit(swh_scheduler):
    swh_scheduler.create_tasks([
        create_task_dict('swh-test-ping', 'oneshot', key='value%d' % i)
        for i in range(10)
    ])

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping', '--limit', '3',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 2 swh-test-ping tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value0'

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value1'

Task 3
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_pending_tasks_before(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task1['next_run'] += datetime.timedelta(days=3)
    task2['next_run'] += datetime.timedelta(days=1)
    swh_scheduler.create_tasks([task1, task2])

    result = invoke(swh_scheduler, False, [
        'task', 'list-pending', 'swh-test-ping', '--before',
        (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 swh-test-ping tasks

Task 2
  Next run: in a day \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task1['next_run'] += datetime.timedelta(days=3, hours=2)
    swh_scheduler.create_tasks([task1, task2])

    swh_scheduler.grab_ready_tasks('swh-test-ping')

    result = invoke(swh_scheduler, False, [
        'task', 'list',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 2 tasks

Task 1
  Next run: in 3 days \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value1'

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_id(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task3 = create_task_dict('swh-test-ping', 'oneshot', key='value3')
    swh_scheduler.create_tasks([task1, task2, task3])

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--task-id', '2',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 tasks

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_id_2(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task3 = create_task_dict('swh-test-ping', 'oneshot', key='value3')
    swh_scheduler.create_tasks([task1, task2, task3])

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--task-id', '2', '--task-id', '3'
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 2 tasks

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value2'

Task 3
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value3'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_type(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-multiping', 'oneshot', key='value2')
    task3 = create_task_dict('swh-test-ping', 'oneshot', key='value3')
    swh_scheduler.create_tasks([task1, task2, task3])

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--task-type', 'swh-test-ping'
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 2 tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value1'

Task 3
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value3'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_limit(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task3 = create_task_dict('swh-test-ping', 'oneshot', key='value3')
    swh_scheduler.create_tasks([task1, task2, task3])

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--limit', '2',
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 2 tasks

Task 1
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value1'

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_before(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task1['next_run'] += datetime.timedelta(days=3, hours=2)
    swh_scheduler.create_tasks([task1, task2])

    swh_scheduler.grab_ready_tasks('swh-test-ping')

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--before',
        (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 tasks

Task 2
  Next run: just now \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value2'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output


def test_list_tasks_after(swh_scheduler):
    task1 = create_task_dict('swh-test-ping', 'oneshot', key='value1')
    task2 = create_task_dict('swh-test-ping', 'oneshot', key='value2')
    task1['next_run'] += datetime.timedelta(days=3, hours=2)
    swh_scheduler.create_tasks([task1, task2])

    swh_scheduler.grab_ready_tasks('swh-test-ping')

    result = invoke(swh_scheduler, False, [
        'task', 'list', '--after',
        (datetime.date.today() + datetime.timedelta(days=2)).isoformat()
    ])

    expected = r'''
\[INFO\] swh.core.config -- Loading config file .*
Found 1 tasks

Task 1
  Next run: in 3 days \(.*\)
  Interval: 1 day, 0:00:00
  Type: swh-test-ping
  Policy: oneshot
  Status: next_run_not_scheduled
  Priority:\x20
  Args:
  Keyword args:
    key: 'value1'

'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), result.output
