# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

from . import ConnectionBase


class BuildConnection(ConnectionBase):
    def insert_build(self, build_id, version, params):
        d = dict(params)
        d['id'] = build_id
        d['version_'] = version

        self._insert_dict(b'builds', d)

    def get_build(self, build_id):
        with self.cursor() as c:
            c.execute(b'SELECT * FROM builds WHERE id=:id', {'id': build_id})

            for row in self._cursor_to_dicts(c):
                return row

            return None

    def get_builds(self, build_ids):
        with self.cursor() as c:
            ids = []
            for i, build_id in enumerate(build_ids):
                ids.append(str(build_id))

                if i % 1000 == 0:
                    c.execute(b'SELECT id, version_ FROM builds WHERE id IN (%s)' %
                        b', '.join(ids))

                    for row in self._cursor_to_dicts(c):
                        yield row

                    ids[:] = []

            if ids:
                c.execute(b'SELECT id, version_ FROM builds WHERE id IN (%s)' %
                    b', '.join(ids))

                for row in self._cursor_to_dicts(c):
                    yield row

    def update_log_parse_version(self, build_id, version):
        with self.cursor() as c:
            c.execute(b'UPDATE builds SET log_parse_version=:v WHERE id=:id',
                {'v': version, 'id': build_id})

