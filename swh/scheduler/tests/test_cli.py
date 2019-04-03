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

from swh.storage.in_memory import Storage

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
        args = ['-C' + config_fd.name, '-l', 'WARNING'] + args
        result = runner.invoke(cli, args)
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


def _fill_storage_with_origins(storage, nb_origins):
    storage.origin_add([
        {
            'type': 'type{}'.format(i),
            'url': 'http://example.com/{}'.format(i),
        }
        for i in range(nb_origins)
    ])


@pytest.fixture
def storage():
    """An instance of swh.storage.in_memory.Storage that gets injected
    into the CLI functions."""
    storage = Storage()
    with patch('swh.scheduler.cli.get_storage') as get_storage_mock:
        get_storage_mock.return_value = storage
        yield storage


@patch('swh.scheduler.cli_utils.TASK_BATCH_SIZE', 3)
def test_task_schedule_origins_dry_run(
        swh_scheduler, storage):
    """Tests the scheduling when origin_batch_size*task_batch_size is a
    divisor of nb_origins."""
    _fill_storage_with_origins(storage, 90)

    result = invoke(swh_scheduler, False, [
        'task', 'schedule_origins', '--dry-run', 'swh-test-ping',
    ])

    # Check the output
    expected = r'''
Scheduled 3 tasks \(30 origins\).
Scheduled 6 tasks \(60 origins\).
Scheduled 9 tasks \(90 origins\).
Done.
'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), \
        repr(result.output)

    # Check scheduled tasks
    tasks = swh_scheduler.search_tasks()
    assert len(tasks) == 0


@patch('swh.scheduler.cli_utils.TASK_BATCH_SIZE', 3)
def test_task_schedule_origins(swh_scheduler, storage):
    """Tests the scheduling when neither origin_batch_size or
    task_batch_size is a divisor of nb_origins."""
    _fill_storage_with_origins(storage, 70)

    result = invoke(swh_scheduler, False, [
        'task', 'schedule_origins', 'swh-test-ping',
        '--batch-size', '20',
    ])

    # Check the output
    expected = r'''
Scheduled 3 tasks \(60 origins\).
Scheduled 4 tasks \(70 origins\).
Done.
'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), \
        repr(result.output)

    # Check scheduled tasks
    tasks = swh_scheduler.search_tasks()
    assert len(tasks) == 4
    assert tasks[0]['arguments']['args'] == [list(range(1, 21))]
    assert tasks[1]['arguments']['args'] == [list(range(21, 41))]
    assert tasks[2]['arguments']['args'] == [list(range(41, 61))]
    assert tasks[3]['arguments']['args'] == [list(range(61, 71))]
    assert all(task['arguments']['kwargs'] == {} for task in tasks)


def test_task_schedule_origins_kwargs(swh_scheduler, storage):
    """Tests support of extra keyword-arguments."""
    _fill_storage_with_origins(storage, 30)

    result = invoke(swh_scheduler, False, [
        'task', 'schedule_origins', 'swh-test-ping',
        '--batch-size', '20',
        'key1="value1"', 'key2="value2"',
    ])

    # Check the output
    expected = r'''
Scheduled 2 tasks \(30 origins\).
Done.
'''.lstrip()
    assert result.exit_code == 0, result.output
    assert re.fullmatch(expected, result.output, re.MULTILINE), \
        repr(result.output)

    # Check scheduled tasks
    tasks = swh_scheduler.search_tasks()
    assert len(tasks) == 2
    assert tasks[0]['arguments']['args'] == [list(range(1, 21))]
    assert tasks[1]['arguments']['args'] == [list(range(21, 31))]
    assert all(task['arguments']['kwargs'] ==
               {'key1': 'value1', 'key2': 'value2'}
               for task in tasks)