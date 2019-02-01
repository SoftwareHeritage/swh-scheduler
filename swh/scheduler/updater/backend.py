# Copyright (C) 2018  The Software Heritage developers
# See the AUTHORS file at the top-level directory of this distribution
# License: GNU General Public License version 3, or any later version
# See top-level LICENSE file for more information


from arrow import utcnow
import psycopg2.pool
import psycopg2.extras

from swh.core.db import BaseDb
from swh.core.db.common import db_transaction, db_transaction_generator
from swh.scheduler.backend import format_query


class SchedulerUpdaterBackend:
    CONFIG_BASE_FILENAME = 'backend/scheduler-updater'
#        'cache_read_limit': ('int', 1000),

    def __init__(self, db, cache_read_limit=1000,
                 min_pool_conns=1, max_pool_conns=10):
        """
        Args:
            db_conn: either a libpq connection string, or a psycopg2 connection

        """
        if isinstance(db, psycopg2.extensions.connection):
            self._pool = None
            self._db = BaseDb(db)
        else:
            self._pool = psycopg2.pool.ThreadedConnectionPool(
                min_pool_conns, max_pool_conns, db,
                cursor_factory=psycopg2.extras.RealDictCursor,
            )
            self._db = None
        self.limit = cache_read_limit

    def get_db(self):
        if self._db:
            return self._db
        return BaseDb.from_pool(self._pool)

    cache_put_keys = ['url', 'cnt', 'last_seen', 'origin_type']

    @db_transaction()
    def cache_put(self, events, timestamp=None, db=None, cur=None):
        """Write new events in the backend.

        """
        cur.execute('select swh_mktemp_cache()')
        db.copy_to(prepare_events(events, timestamp),
                   'tmp_cache', self.cache_put_keys, cur=cur)
        cur.execute('select swh_cache_put()')

    cache_read_keys = ['id', 'url', 'origin_type', 'cnt', 'first_seen',
                       'last_seen']

    @db_transaction_generator()
    def cache_read(self, timestamp=None, limit=None, db=None, cur=None):
        """Read events from the cache prior to timestamp.

        Note that limit=None does not mean 'no limit' but use the default
        limit (see cache_read_limit constructor argument).

        """
        if not timestamp:
            timestamp = utcnow()

        if not limit:
            limit = self.limit

        q = format_query('select {keys} from swh_cache_read(%s, %s)',
                         self.cache_read_keys)
        cur.execute(q, (timestamp, limit))
        yield from cur.fetchall()

    @db_transaction()
    def cache_remove(self, entries, db=None, cur=None):
        """Clean events from the cache

        """
        q = 'delete from cache where url in (%s)' % (
            ', '.join(("'%s'" % e for e in entries)), )
        cur.execute(q)


def prepare_events(events, timestamp=None):
    if timestamp is None:
        timestamp = utcnow()
    outevents = []
    urls = []
    for e in events:
        event = e.get()
        url = event['url'].strip()
        if event['last_seen'] is None:
            event['last_seen'] = timestamp
        event['url'] = url

        if url in urls:
            idx = urls.index(url)
            urls.append(urls.pop(idx))
            pevent = outevents.pop(idx)
            event['cnt'] += pevent['cnt']
            event['last_seen'] = max(
                event['last_seen'], pevent['last_seen'])
        else:
            urls.append(url)
        outevents.append(event)
    return outevents
