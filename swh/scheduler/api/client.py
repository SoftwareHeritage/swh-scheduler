# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from swh.core.api import SWHRemoteAPI


class RemoteScheduler(SWHRemoteAPI):
    """Proxy to a remote scheduler API

    """
    def close_connection(self):
        return self.post('close_connection', {})

    def set_status_tasks(self, task_ids, status='disabled', next_run=None):
        return self.post('set_status_tasks', dict(
            task_ids=task_ids, status=status, next_run=next_run))

    def create_task_type(self, task_type):
        return self.post('create_task_type', {'task_type': task_type})

    def get_task_type(self, task_type_name):
        return self.post('get_task_type', {'task_type_name': task_type_name})

    def get_task_types(self):
        return self.post('get_task_types', {})

    def create_tasks(self, tasks):
        return self.post('create_tasks', {'tasks': tasks})

    def disable_tasks(self, task_ids):
        return self.post('disable_tasks', {'task_ids': task_ids})

    def get_tasks(self, task_ids):
        return self.post('get_tasks', {'task_ids': task_ids})

    def get_task_runs(self, task_ids, limit=None):
        return self.post(
            'get_task_runs', {'task_ids': task_ids, 'limit': limit})

    def search_tasks(self, task_id=None, task_type=None, status=None,
                     priority=None, policy=None, before=None, after=None,
                     limit=None):
        return self.post('search_tasks', dict(
            task_id=task_id, task_type=task_type, status=status,
            priority=priority, policy=policy, before=before, after=after,
            limit=limit))

    def peek_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         num_tasks_priority=None):
        return self.post('peek_ready_tasks', {
            'task_type': task_type,
            'timestamp': timestamp,
            'num_tasks': num_tasks,
            'num_tasks_priority': num_tasks_priority,
        })

    def grab_ready_tasks(self, task_type, timestamp=None, num_tasks=None,
                         num_tasks_priority=None):
        return self.post('grab_ready_tasks', {
            'task_type': task_type,
            'timestamp': timestamp,
            'num_tasks': num_tasks,
            'num_tasks_priority': num_tasks_priority,
        })

    def schedule_task_run(self, task_id, backend_id, metadata=None,
                          timestamp=None):
        return self.post('schedule_task_run', {
            'task_id': task_id,
            'backend_id': backend_id,
            'metadata': metadata,
            'timestamp': timestamp,
        })

    def mass_schedule_task_runs(self, task_runs):
        return self.post('mass_schedule_task_runs', {'task_runs': task_runs})

    def start_task_run(self, backend_id, metadata=None, timestamp=None):
        return self.post('start_task_run', {
            'backend_id': backend_id,
            'metadata': metadata,
            'timestamp': timestamp,
        })

    def end_task_run(self, backend_id, status, metadata=None, timestamp=None):
        return self.post('end_task_run', {
            'backend_id': backend_id,
            'status': status,
            'metadata': metadata,
            'timestamp': timestamp,
        })

    def filter_task_to_archive(self, after_ts, before_ts, limit=10,
                               last_id=-1):
        return self.post('filter_task_to_archive', {
            'after_ts': after_ts,
            'before_ts': before_ts,
            'limit': limit,
            'last_id': last_id,
        })

    def delete_archived_tasks(self, task_ids):
        return self.post('delete_archived_tasks', {'task_ids': task_ids})
