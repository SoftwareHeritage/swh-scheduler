# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from arrow import utcnow
from swh.core.config import SWHConfig
from swh.scheduler.backend import DbBackend, autocommit


class SchedulerUpdaterBackend(SWHConfig, DbBackend):
    CONFIG_BASE_FILENAME = 'scheduler-updater'
    DEFAULT_CONFIG = {
        'scheduling_updater_db': (
            'str', 'dbname=softwareheritage-scheduler-updater-dev'),
        'time_window': ('str', '1 hour'),
    }

    def __init__(self, **override_config):
        super().__init__()
        self.config = self.parse_config_file(global_config=False)
        self.config.update(override_config)
        self.db = None
        self.db_conn_dsn = self.config['scheduling_updater_db']
        self.time_window = self.config['time_window']
        self.reconnect()

    cache_put_keys = ['url', 'rate', 'last_seen']

    @autocommit
    def cache_put(self, events, timestamp=None, cursor=None):
        if timestamp is None:
            timestamp = utcnow()

        def prepare_events(events):
            for e in events:
                event = e.get()
                seen = event['last_seen']
                if seen is None:
                    event['last_seen'] = timestamp
                yield event

        cursor.execute('select swh_mktemp_cache()')
        self.copy_to(prepare_events(events),
                     'tmp_cache', self.cache_put_keys, cursor)
        cursor.execute('select swh_cache_put()')

    # @autocommit
    # def cache_get(self, event, cursor=None):
    #     pass

    # @autocommit
    # def cache_remove(self, event, cursor=None):
    #     pass

    cache_read_keys = ['id', 'url']

    @autocommit
    def cache_read(self, timestamp, limit=100, cursor=None):
        q = self._format_query("""select {keys}
               from cache
               where %s - interval %s <= last_seen and last_seen <= %s
               limit %s
            """, self.cache_read_keys)
        cursor.execute(q, (timestamp, self.time_window, timestamp, limit))
        return cursor.fetchall()
