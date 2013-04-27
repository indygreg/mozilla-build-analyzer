# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals


class ConnectionBase(object):
    def __init__(self, connection):
        self.c = connection

    def _insert_dict(self, table, d):
        keys = [str(k) for k in sorted(d)]
        query = b'INSERT INTO %s (%s) VALUES (%s)' % (
            table, b', '.join(keys), b', '.join(b':%s' % k for k in keys))

        # There is a bug in cql where booleans aren't properly encoded in
        # execute(). However, they work for execute_prepared(), so we use that.
        c = self.c.cursor()
        q = c.prepare_query(query)
        c.execute_prepared(q, d)
        c.close()

    def _cursor_to_dicts(self, c):
        names = c.name_info

        for row in c:
            data = {}
            for i, (name, cls) in enumerate(names):
                data[name] = row[i]

            yield data
