# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from arrow import utcnow
from swh.core.config import SWHConfig
from swh.scheduler.backend import DbBackend, autocommit


class SchedulerUpdaterBackend(SWHConfig, DbBackend):
    CONFIG_BASE_FILENAME = 'backend/scheduler-updater'
    DEFAULT_CONFIG = {
        'scheduling_updater_db': (
            'str', 'dbname=softwareheritage-scheduler-updater-dev'),
        'cache_read_limit': ('int', 1000),
    }

    def __init__(self, **override_config):
        super().__init__()
        if override_config:
            self.config = override_config
        else:
            self.config = self.parse_config_file(global_config=False)
        self.db = None
        self.db_conn_dsn = self.config['scheduling_updater_db']
        self.limit = self.config['cache_read_limit']
        self.reconnect()

    cache_put_keys = ['url', 'cnt', 'last_seen', 'origin_type']

    @autocommit
    def cache_put(self, events, timestamp=None, cursor=None):
        """Write new events in the backend.

        """
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
                     'tmp_cache', self.cache_put_keys, cursor=cursor)
        cursor.execute('select swh_cache_put()')

    cache_read_keys = ['id', 'url', 'origin_type', 'cnt', 'first_seen',
                       'last_seen']

    @autocommit
    def cache_read(self, timestamp=None, limit=None, cursor=None):
        """Read events from the cache prior to timestamp.

        """
        if not timestamp:
            timestamp = utcnow()

        if not limit:
            limit = self.limit

        q = self._format_query('select {keys} from swh_cache_read(%s, %s)',
                               self.cache_read_keys)
        cursor.execute(q, (timestamp, limit))
        for r in cursor.fetchall():
            r['id'] = r['id'].tobytes()
            yield r

    @autocommit
    def cache_remove(self, entries, cursor=None):
        """Clean events from the cache

        """
        q = 'delete from cache where url in (%s)' % (
            ', '.join(("'%s'" % e for e in entries)), )
        cursor.execute(q)
