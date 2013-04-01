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

    # Derived data.

    # We have a catch-all super column family for useful indices. Keys are
    # the name of the index. Super columns are the thing being mapped from.
    # Columns are what we are lists of things we are mapping to. Values
    # are typically empty.
    'indices': {
        'comment': 'General bucket for useful indexes.',
        'column_type': 'Super',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'subcomparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'simple_indices': {
        'comment': 'Version of indices with a regular column family.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'counters': {
        'comment': 'Holds counters in a regular column family.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'CounterColumnType',
    },
    'super_counters': {
        'comment': 'Holds counters in a super column family.',
        'column_type': 'Super',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'subcomparator_type': UTF8_TYPE,
        'default_validation_class': 'CounterColumnType',
    },
    'build_timelines': {
        'comment': 'Records information about various phases of builds.',
        'column_type': 'Super',
        'key_validation_class': 'UTF8Type',
        'comparator_type': LONG_TYPE,
        'subcomparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
}

COLUMN_TYPES = {}

BUILD_METADATA_INDICES = [
    'builder_category_to_builder_ids',
    'builder_category_to_build_ids',
    'builder_id_to_build_ids',
    'builder_id_to_slave_ids',
    'builder_name_to_build_ids',
    'master_id_to_build_ids',
    'master_id_to_slave_ids',
    'slave_id_to_build_ids',
]

BUILD_METADATA_COUNTERS = [
    'builder_number',
    'builder_duration',
]

BUILD_METADATA_SUPER_COUNTERS = [
    'builder_number_by_day',
    'builder_duration_by_day',
    'builder_number_by_category',
    'builder_duration_by_category',
    'builder_number_by_day_and_category',
    'builder_duration_by_day_and_category',
]

LOG_METADATA_INDICES = [
    'build_step_name_to_build_ids',
]

LOG_METADATA_COUNTERS = [
    'build_step_number',
    'build_step_duration',
]

LOG_METADATA_SUPER_COUNTERS = [
    'build_step_number_by_category',
    'build_step_duration_by_category',
    'build_step_number_by_day',
    'build_step_duration_by_day',
    'build_step_number_by_day_and_category',
    'build_step_duration_by_day_and_category',
]

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
            timeout=30, *args, **kwargs)

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

        indices = ColumnFamily(self.pool, 'simple_indices')
        indices.insert('files', {key: ''})

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

    def filenames(self):
        """Obtain the keys of all stored files."""
        cf = ColumnFamily(self.pool, 'simple_indices')
        return self._all_column_names_in_row(cf, 'files')

    def truncate_build_metadata(self):
        """Truncates all derived build metadata.

        This bulk removes all build metadata and should not be performed
        unless you want to reload all derived data!
        """
        for cf in ['slaves', 'masters', 'builders', 'builds']:
            cf = ColumnFamily(self.pool, cf)
            cf.truncate()

        cf = ColumnFamily(self.pool, 'indices')
        for key in BUILD_METADATA_INDICES:
            cf.remove(key)

        cf = ColumnFamily(self.pool, 'counters')
        for key in BUILD_METADATA_COUNTERS:
            cf.remove(key)

        cf = ColumnFamily(self.pool, 'super_counters')
        for key in BUILD_METADATA_SUPER_COUNTERS:
            cf.remove(key)

    def truncate_log_metadata(self):
        for cf in ['build_timelines']:
            cf = ColumnFamily(self.pool, cf)
            cf.truncate()

        cf = ColumnFamily(self.pool, 'indices')
        for key in LOG_METADATA_INDICES:
            cf.remove(key)

        cf = ColumnFamily(self.pool, 'counters')
        for key in LOG_METADATA_COUNTERS:
            cf.remove(key)

        cf = ColumnFamily(self.pool, 'super_counters')
        for key in LOG_METADATA_SUPER_COUNTERS:
            cf.remove(key)

        cf = ColumnFamily(self.pool, 'builds')
        batch = cf.batch()
        # Remove log parsing state from builds.
        for key, cols in cf.get_range(columns=['log_parsing_version']):
            if 'log_parsing_version' not in cols:
                continue

            batch.remove(key, ['log_parsing_version'])

        batch.send()

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
        return self._all_columns_in_supercolumn_column(cf,
            'builder_category_to_builder_ids', category)

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
        return self._all_columns_in_supercolumn_column(cf,
            'slave_id_to_build_ids', slave_id)

    def build_ids_in_category(self, category):
        """Obtain build IDs having the specified category."""
        cf = ColumnFamily(self.pool, 'indices')
        return self._all_columns_in_supercolumn_column(cf,
            'builder_category_to_build_ids', category)

    def build_ids_with_builder_name(self, builder_name):
        cf = ColumnFamily(self.pool, 'indices')
        return self._all_columns_in_supercolumn_column(cf,
            'builder_name_to_build_ids', builder_name)

    def build_ids_with_builder_id(self, builder_id):
        cf = ColumnFamily(self.pool, 'indices')
        return self._all_columns_in_supercolumn_column(cf,
            'builder_id_to_builds_ids', builder_id)

    def build_from_id(self, build_id):
        """Obtain information about a build from its ID."""
        cf = ColumnFamily(self.pool, 'builds')
        try:
            return cf.get(build_id)
        except NotFoundException:
            return None

    def build_log(self, build_id):
        """Obtain the raw log for a job from its ID."""
        info = self.build_from_id(build_id)
        if not info:
            return None

        if 'log_url' not in info:
            return None

        return self.file_data(info['log_url'])

    def get_counts(self, name):
        """Obtain current counts from a counter in the counts column family."""
        cf = ColumnFamily(self.pool, 'counters')
        return self._all_columns_in_row(cf, name)

    def _all_columns_in_row(self, cf, key):
        try:
            start_column =''
            while True:
                result = cf.get(key, column_start=start_column,
                    column_finish='', column_count=1000)

                for k, v in result.items():
                    yield k, v
                    start_column = k

                if len(result) != 1000:
                    break

        except NotFoundException:
            pass

    def _all_column_names_in_row(self, cf, key):
        """Generator for names of all columns in a column family."""
        try:
            start_column = ''
            while True:
                result = cf.get(key, column_start=start_column,
                    column_finish='', column_count=1000)

                for column_name in result.keys():
                    yield column_name
                    start_column = column_name

                if len(result) != 1000:
                    break

        except NotFoundException:
            pass

    def _all_columns_in_supercolumn_column(self, cf, key, column):
        """Generator for the names of all columns in a supercolumn column."""

        try:
            start_column =''
            while True:
                result = cf.get(key, column_start=start_column, column_finish='',
                    super_column=column, column_count=1000)

                for column_name in result.keys():
                    yield column_name
                    start_column = column_name

                if len(result) != 1000:
                    break

        except NotFoundException:
            pass

