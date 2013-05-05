# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import collections
import threading

from contextlib import contextmanager

import cql


class ConnectionPool(object):
    '''Our own connection pool because cql's isn't ready for prime time.'''
    def __init__(self, host, port, keyspace, create=True, size=5,
        *args, **kwargs):
        self._conns = collections.deque()
        self._sem = threading.Semaphore(size)

        for i in range(size):
            self._conns.append(cql.connect(host, port, keyspace,
                cql_version='3.0.1', *args, **kwargs))

        self.keyspace = self._conns[0].keyspace

    @contextmanager
    def conn(self):
        with self._sem:
            conn = self._conns.popleft()
            try:
                yield conn
            finally:
                self._conns.append(conn)


class ConnectionBase(object):
    def __init__(self, pool):
        self._pool = pool
        self.keyspace = pool.keyspace

    @contextmanager
    def cursor(self):
        with self._pool.conn() as conn:
            c = conn.cursor()
            yield c
            c.close()

    def _insert_dict(self, table, d):
        keys = [str(k) for k in sorted(d)]
        query = b'INSERT INTO %s (%s) VALUES (%s)' % (
            table, b', '.join(keys), b', '.join(b':%s' % k for k in keys))

        # There is a bug in cql where booleans aren't properly encoded in
        # execute(). However, they work for execute_prepared(), so we use that.
        with self.cursor() as c:
            q = c.prepare_query(query)
            c.execute_prepared(q, d)

    def _cursor_to_dicts(self, c):
        names = c.name_info

        for row in c:
            data = {}
            for i, (name, cls) in enumerate(names):
                data[name] = row[i]

            yield data

