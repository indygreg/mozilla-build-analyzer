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
    DATE_TYPE,
    INT_TYPE,
    KEYS_INDEX,
    LONG_TYPE,
    UTF8_TYPE,
)

from pycassa import NotFoundException


COLUMN_FAMILIES = {
    # Blobs are where we store large, seldomly read and written binary blobs
    # of data. This is where we put raw build logs and summaries of builds.
    'blobs': {
        'comment': 'Holds opaque binary blobs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': 'UTF8Type',
        'default_validation_class': 'BytesType',
        'column_validation_classes': {
            'version': INT_TYPE,
            'sha1': BYTES_TYPE,
            'size': INT_TYPE,
            'chunk_count': INT_TYPE,
            'chunk_size': INT_TYPE,
        },
        # While slower, the DeflateCompressor gives much better compression
        # for what we store in here (logs) than Snappy (2-2.5x better).
        'compression_options': {
            'sstable_compression': 'DeflateCompressor',
            'chunk_length_kb': '512',
        },
    },
    # Files are essentially an index into blobs. They are where we record which
    # files have been fetched and their state in the blobs column family. There
    # is significant overlap between the metadata in here and blobs. The reason
    # is this column is quick to read from because rows are small. Blobs should
    # only be read when accessing the content of a specific file.
    'files': {
        'comment': 'Stores raw files.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
        'column_validation_classes': {
            'version': INT_TYPE,
            'sha1': BYTES_TYPE,
            'size': INT_TYPE,
            'mtime': DATE_TYPE,
            'compression_state': UTF8_TYPE,
            'compressed_size': INT_TYPE,
        },
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
        'column_validation_classes': {
            'duration': INT_TYPE,
        },
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
    'build_id_to_duration',
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
    'build_duration_by_builder_id',
    'build_duration_by_builder_name',
    'build_duration_by_builder_category',
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

DEFAULT_BLOB_CHUNK_SIZE = 1048576

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
            timeout=90, pool_size=15, *args, **kwargs)

    def store_blob(self, key, content, chunk_size=DEFAULT_BLOB_CHUNK_SIZE):
        cf = ColumnFamily(self.pool, 'blobs')
        chunks = len(content) / chunk_size + 1

        sha1 = hashlib.sha1()
        sha1.update(content)

        offset = 0
        i = 1
        while True:
            b = content[offset:offset + chunk_size]
            # We prefix each part with "z" so the big chunks come at the end of
            # the row and our initial read for all the metadata doesn't span
            # excessive pages on disk.
            cf.insert(key, {'z:%04d' % i: b})

            if len(b) < chunk_size:
                break

            offset += chunk_size
            i += 1

        cf.insert(key, {
            'version': 1,
            'sha1': sha1.digest(),
            'size': len(content),
            'chunk_size': chunk_size,
            'chunk_count': chunks,
        })

        indices = ColumnFamily(self.pool, 'simple_indices')
        indices.insert('blobs', {key: ''})
        indices.insert('blob_size', {key: str(len(content))})

        return sha1.digest()

    def get_blob(self, key):
        cf = ColumnFamily(self.pool, 'blobs')

        row = cf.get(key, columns=['version', 'sha1', 'size', 'chunk_size',
            'chunk_count', 'z:0001'])

        if 'version' not in row:
            raise Exception('No version column in blob.')

        if row['version'] != 1:
            raise Exception('Unknown version: %d' % row['version'])

        if 'z:0001' not in row:
            raise Exception('No data in blob.')

        sha1 = hashlib.sha1()
        sha1.update(row['z:0001'])

        if len(row['z:0001']) == row['size']:
            if sha1.digest() != row['sha1']:
                raise Exception('SHA-1 validation failed.')

            return row['z:0001']

        chunks = [row['z:0001']]

        for i in range(2, row['chunk_count'] + 1):
            col = 'z:%04d' % i
            chunk = cf.get(key, columns=[col])[col]
            sha1.update(chunk)
            chunks.append(chunk)

        if sha1.digest() != row['sha1']:
            raise Exception('SHA-1 validation failed.')

        return b''.join(chunks)

    def store_file(self, filename, content, mtime=-1, compression_state=None,
        compressed_size=None):

        cf = ColumnFamily(self.pool, 'files')

        compression_state = compression_state or 'unknown'
        assert compression_state in ('none', 'unknown', 'gzip')

        sha1 = self.store_blob(filename, content)

        cols = {
            'version': 1,
            'sha1': sha1,
            'size': len(content),
            'mtime': mtime,
            'compression_state': compression_state,
        }

        if compressed_size is not None:
            cols['compressed_size'] = compressed_size

        cf.insert(filename, cols)

        indices = ColumnFamily(self.pool, 'simple_indices')
        indices.insert('files', {filename: ''})

    def file_metadata(self, keys):
        """Obtain metadata for a stored file.

        Argument is an iterable of file keys whose data to obtain.
        """
        cf = ColumnFamily(self.pool, 'files')

        return cf.multiget(keys)

    def file_data(self, key):
        cf = ColumnFamily(self.pool, 'files')

        try:
            columns = cf.get(key, columns=['compression_state'])
            raw = self.get_blob(key)

            if columns['compression_state'] in ('none', 'unknown'):
                return raw

            raise Exception('We do not currently handle decompression.')

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

    def build_durations(self, build_ids=None):
        cf = ColumnFamily(self.pool, 'simple_indices')

        if build_ids is None:
            values = self._all_columns_in_row(cf, 'build_id_to_duration')
        else:
            values = self._column_values(cf, 'build_id_to_duration', build_ids)

        for col, value in values:
            yield col, int(value)

    def build_durations_with_builder_name(self, builder):
        cf = ColumnFamily(self.pool, 'indices')

        for col, value in self._all_columns_in_supercolumn_column(cf,
            'build_duration_by_builder_name', builder, values=True):

            yield col, int(value)

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

    def _column_values(self, cf, key, columns):
        try:
            fetch = []
            for column in columns:
                fetch.append(column)

                if len(fetch) == 1000:
                    result = cf.get(key, columns=fetch)

                    for col in fetch:
                        yield col, result.get(col, None)

                    fetch = []

            if len(fetch):
                result = cf.get(key, columns=fetch)

                for col in fetch:
                    yield col, result.get(col, None)

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

    def _all_columns_in_supercolumn_column(self, cf, key, column, values=False):
        """Generator for the names of all columns in a supercolumn column."""

        try:
            start_column =''
            while True:
                result = cf.get(key, column_start=start_column, column_finish='',
                    super_column=column, column_count=1000)

                for column_name, column_value in result.iteritems():
                    if values:
                        yield column_name, column_value
                    else:
                        yield column_name

                    start_column = column_name

                if len(result) != 1000:
                    break

        except NotFoundException:
            pass

