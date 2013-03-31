# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import gzip
import hashlib

import pycassa

from StringIO import StringIO

from pycassa.columnfamily import ColumnFamily

from pycassa.system_manager import (
    BYTES_TYPE,
    KEYS_INDEX,
    LONG_TYPE,
    UTF8_TYPE,
)

from pycassa import NotFoundException


COLUMN_FAMILIES = {
    # We have a catch-all super column family for useful indices. Keys are
    # the name of the index. Super columns are the thing being mapped from.
    # Columns are what we are lists of things we are mapping to. Values
    # are typically empty.
    'indices': {
        'comment': 'General bucket for useful indexes.',
        'column_type': 'Super',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'subcomparator': 'UTF8Type',
        'default_validation_class': 'UTF8Type',
    },
    'builders': {
        'comment': 'Information about different job types (builders).',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'builds': {
        'comment': 'Holds information about specific build jobs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'slaves': {
        'comment': 'Holds information about slaves that run jobs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'masters': {
        'comment': 'Describes buildbot masters.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'files': {
        'comment': 'Stores raw files.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'BytesType',
        'column_validation_classes': {
            'storage_format': LONG_TYPE,
            'compression': UTF8_TYPE,
            'sha1': BYTES_TYPE,
            'size': LONG_TYPE,
            'mtime': LONG_TYPE,
            'data': BYTES_TYPE,
        },
    },
}

COLUMN_TYPES = {}


class Connection(object):
    def __init__(self):
        self.pool = None

    def connect(self, keyspace, *args, **kwargs):
        """Connect to a Cassandra cluster and ensure state is sane."""

        servers = kwargs.get('servers', ['localhost'])

        manager = pycassa.system_manager.SystemManager(server=servers[0])

        # TODO we should split schema creation into its own method with full
        # configuration options.
        if keyspace not in manager.list_keyspaces():
            manager.create_keyspace(keyspace,
                pycassa.system_manager.SIMPLE_STRATEGY,
                {'replication_factor': '1'})

        cfs = manager.get_keyspace_column_families(keyspace)

        for name, props in COLUMN_FAMILIES.items():
            if name not in cfs:
                manager.create_column_family(keyspace, name, **props)
                continue

            # The logic here is likely crap. Surely this problem has been
            # solved before...
            existing = cfs[name]
            existing_metadata = {d.name: d for d in existing.column_metadata}

            for column, column_type in COLUMN_TYPES.get(name, {}).items():
                e = existing_metadata.get(column)
                if e:
                    continue

                manager.alter_column(keyspace, name, column, column_type)

        self.pool = pycassa.pool.ConnectionPool(keyspace, server_list=servers,
            *args, **kwargs)

    def store_file(self, key, content, compression=None, mtime=-1):
        cf = ColumnFamily(self.pool, 'files')
        sha1 = hashlib.sha1()
        sha1.update(content)

        compression = compression or 'none'
        assert compression in ('none', 'gzip', 'bzip')

        cf.insert(key, {
            'storage_format': 1,
            'sha1': sha1.digest(),
            'compression': compression,
            'size': len(content),
            'mtime': mtime,
            'data': content,
        })

    def file_metadata(self, keys):
        """Obtain metadata for a stored file.

        Argument is an iterable of file keys whose data to obtain.
        """
        cf = ColumnFamily(self.pool, 'files')

        result = cf.multiget(keys,
            columns=['storage_format', 'sha1', 'compression', 'mtime', 'size'])

        return result

    def file_data(self, key):
        cf = ColumnFamily(self.pool, 'files')

        try:
            columns = cf.get(key, columns=['data', 'compression'])
            if columns['compression'] == 'none':
                return columns['data']

            if columns['compression'] == 'gzip':
                s = StringIO(columns['data'])
                return gzip.GzipFile(fileobj=s).read()

            raise Exception('Unknown compression: %s' % columns['compression'])

        except NotFoundException:
            return None

    def truncate_build_metadata(self):
        """Truncates all derived build metadata.

        This bulk removes all build metadata and should not be performed
        unless you want to reload all derived data!
        """
        for cf in ['slaves', 'masters', 'builders', 'builds']:
            cf = ColumnFamily(self.pool, cf)
            cf.truncate()

    def builders(self):
        """Obtain info about all builders."""
        cf = ColumnFamily(self.pool, 'builders')
        for key, cols in cf.get_range(columns=['category', 'master', 'name']):
            yield key, cols['name'], cols['category'], cols['master']

    def get_builder(self, builder_id):
        """Obtain info about a builder from its ID."""
        cf = ColumnFamily(self.pool, 'builders')
        try:
            return cf.get(builder_id)
        except NotFoundException:
            return None

    def builder_ids_in_category(self, category):
        cf = ColumnFamily(self.pool, 'indices')
        return cf.get('builder_category_to_builder_ids',
            super_column=category).keys()

    def slaves(self):
        """Obtain basic metadata about all slaves."""

        cf = ColumnFamily(self.pool, 'slaves')

        for key, cols in cf.get_range(columns=['name']):
            yield key, cols['name']

    def slave_id_from_name(self, name):
        # TODO index.
        for slave_id, slave_name in self.slaves():
            if slave_name == name:
                return slave_id

        return None

    def build_ids_on_slave(self, slave_id):
        """Obtain all build IDs that were performed on the slave."""
        cf = ColumnFamily(self.pool, 'indices')
        try:
            return cf.get('slave_id_to_build_ids',
                super_column=slave_id).keys()
        except NotFoundException:
            return []

    def build_ids_in_category(self, category):
        """Obtain build IDs having the specified category."""
        cf = ColumnFamily(self.pool, 'indices')
        try:
            return cf.get('builder_category_to_build_ids',
                super_column=category).keys()
        except NotFoundException:
            return []

    def build_from_id(self, build_id):
        """Obtain information about a build from its ID."""
        cf = ColumnFamily(self.pool, 'builds')
        try:
            return cf.get(build_id)
        except NotFoundException:
            return None

    def raw_log(self, job_id):
        """Obtain the raw log for a job from its ID."""
        cf = ColumnFamily(self.pool, 'raw_job_logs')

        try:
            cols = cf.get(job_id)
        except NotFoundException:
            return None

        # We store logs compressed, so uncompress it.
        raw = StringIO(cols['log'])
        gz = gzip.GzipFile(fileobj=raw)

        return gz.read()

