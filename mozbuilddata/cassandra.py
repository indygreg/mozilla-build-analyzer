# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import bz2
import cql
import datetime
import hashlib
import zlib

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

from .connection.builds import BuildConnection


TABLES = {
    'builders': b'''
        CREATE TABLE builders (
            id int PRIMARY KEY,
            name text,
            category text,
            master int,
            slaves set<int>,
            builds set<int>,
        )
        WITH comment='Describes individual job types.'
    ''',

    'builder_categories': b'''
        CREATE TABLE builder_categories (
            category text PRIMARY KEY,
            builders set<int>,
            builds set<int>,
            build_durations map<int, varint>,
        )
        WITH comment='Information about builder categories.'
    ''',

    'builder_counters': b'''
        CREATE TABLE builder_counters (
            id int PRIMARY KEY,
            total_number counter,
            total_duration counter,
        )
        WITH comment='Builder ID to builder number counter.'
    ''',

    'builder_daily_counters': b'''
        CREATE TABLE builder_daily_counters (
            id int,
            day timestamp,
            number counter,
            duration counter,
            PRIMARY KEY (id, day),
        )
        WITH comment='Builder ID to per-day counters.'
    ''',

    'builder_category_counters': b'''
        CREATE TABLE builder_category_counters (
            category text PRIMARY KEY,
            number counter,
            duration counter,
        )
        WITH comment='Builder category to counters.'
    ''',

    'builder_category_daily_counters': b'''
        CREATE TABLE builder_category_daily_counters (
            category text,
            day timestamp,
            number counter,
            duration counter,
            PRIMARY KEY (category, day),
        )
        WITH comment='Builder category and day to counters.'
    ''',

    'slaves': b'''
        CREATE TABLE slaves (
            id int PRIMARY KEY,
            name text,
            builds set<int>,
            build_events map<timestamp, text>,
        )
        WITH comment='Describes machines that execute jobs.'
    ''',

    'masters': b'''
        CREATE TABLE masters (
            id int PRIMARY KEY,
            name text,
            url text,
            builders set<int>,
        )
        WITH comment='Describes machines coordinating slaves.'
    ''',

    'builds': b'''
        CREATE TABLE builds (
            id int PRIMARY KEY,
            version_ varint,

            // Top-level items.
            builder_id varint,
            builder_category text,
            build_number varint,
            master_id varint,
            slave_id varint,
            request_time timestamp,
            start_time timestamp,
            end_time timestamp,
            result int,

            // Properties.
            app text,
            app_name text,
            app_version text,
            base_dir text,
            branch text,
            build_dir text,
            build_id text,
            build_filename text,
            build_url text,
            build_uid text,
            builder_name text,
            comments text,
            comm_revision text,
            compare_locales_revision text,
            complete_mar_filename text,
            complete_mar_hash text,
            complete_mar_size varint,
            complete_mar_url text,
            complete_snippet_filename text,
            config_file text,
            config_revision text,
            en_revision text,
            exe_dir text,
            exe_path text,
            fennec_ids_filename text,
            fennec_ids_url text,
            filename text,
            file_path text,
            file_url text,
            foopy_type text,
            forced_clobber boolean,
            fx_revision text,
            gaia_revision text,
            gecko_revision text,
            got_revision text,
            hash_type text,
            hostutils_filename text,
            hostutils_url text,
            http_port varint,
            ini_path text,
            installer_hash text,
            installer_filename text,
            installer_size varint,
            js_shell_url text,
            l10n_revision text,
            locale text,
            locales set<text>,
            log_url text,
            mozmill_virtualenv_setup text,
            moz_revision text,
            num_ctors varint,
            nightly_build boolean,
            package_filename text,
            package_hash text,
            package_size varint,
            package_url text,
            partial_mar_filename text,
            partial_mar_hash text,
            partial_mar_size varint,
            partial_mar_url text,
            partial_snippet_filename text,
            periodic_clobber boolean,
            pgo_build boolean,
            platform text,
            previous_build_id text,
            previous_ini_path text,
            previous_mar_filename text,
            product text,
            products text,
            project text,
            purge_actual text,
            purge_target text,
            purged_clobber boolean,
            reason text,
            release_config text,
            release_tag text,
            remote_process_name text,
            repository text,
            repo_path text,
            request_ids list<varint>,
            revision text,
            robocop_apk_url text,
            robocop_filename text,
            robocop_url text,
            scheduler text,
            script_repo_revision text,
            slave_build_dir text,
            slow_tests boolean,
            sourcestamp text,
            ssl_port varint,
            stage_platform text,
            sut_ip inet,
            symbols_filename text,
            symbols_url text,
            tests_filename text,
            tests_url text,
            tools_dir text,
            tools_revision text,
            tree text,
            unsigned_apk_url text,
            upload_host text,
            upload_ssh_key text,
            upload_user text,
            version text,
            vsize varint,
            who text,

            duration varint,
            master_url text,
            slave_name text,
        )
        WITH comment='Describes individual build jobs.'
    ''',

    # Files are essentially an index into blobs. They are where we record which
    # files have been fetched and their state in the blobs column family. There
    # is significant overlap between the metadata in here and blobs. The reason
    # is this column is quick to read from because rows are small. Blobs should
    # only be read when accessing the content of a specific file.
    'files': b'''
        CREATE TABLE files (
            name text PRIMARY KEY,
            version varint,
            mtime timestamp,
            stored_sha1 blob,
            stored_size varint,
            transformation text,
            original_size varint,
            original_sha1 blob,
            chunk_count varint,
            chunk_size varint,
        )
        WITH comment='Stored files (logs, packages, etc.)'
    ''',

    # Blobs are where we store large, seldomly read and written binary blobs
    # of data. This is where we put raw build logs and summaries of builds.
    # While slower, the DeflateCompressor gives much better compression
    # for what we store in here (logs) than Snappy (2-2.5x better).
    'file_chunks': b'''
        CREATE TABLE file_chunks (
            name text,
            i int,
            chunk blob,
            PRIMARY KEY (name, i)
        )
        WITH comment='Holds opaque binary chunks belonging to files.'
        AND compression = {'sstable_compression': ''}
    ''',

}

INDICES = {
    'builder_category': b'CREATE INDEX builder_category ON builders (category)',
}

COLUMN_TYPES = {}

BUILDER_TABLES = [
    b'masters',
    b'slaves',
    b'builders',
    b'builder_categories',
    b'builder_counters',
    b'builder_daily_counters',
    b'builder_category_counters',
    b'builder_category_daily_counters',
    b'builds',
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


def connect(host, port, keyspace, *args, **kwargs):
    c = cql.connect(host, port, keyspace, cql_version='3.0.0', *args, **kwargs)

    pool = pycassa.pool.ConnectionPool(keyspace, server_list=[host],
        timeout=90, pool_size=15, *args, **kwargs)

    return Connection(c, pool)


class Connection(object):
    def __init__(self, connection, pool):
        self.c = connection
        self.pool = pool

        c = self.c.cursor()
        c.execute(b'SELECT * from system.schema_keyspaces WHERE keyspace_name=:ks',
            {'ks':connection.keyspace})
        if not c.rowcount:
            raise Exception('Please create the %s keyspace.' %
                connection.keyspace)

        c = self.c.cursor()
        c.execute(b'SELECT columnfamily_name FROM system.schema_columnfamilies '
            b'WHERE keyspace_name=:ks', {'ks': self.c.keyspace})
        cf_names = set()
        for row in c:
            cf_names.add(row[0])

        for table, create in TABLES.items():
            if table not in cf_names:
                c.execute(create)

        c.execute(b'SELECT index_name FROM system.schema_columns '
            b'WHERE keyspace_name=:ks', {'ks': self.c.keyspace})
        indices = set()
        for row in c:
            indices.add(row[0])

        for index, create in INDICES.items():
            if index not in indices:
                c.execute(create)

        c.close()

        self.builds = BuildConnection(self.c)

    def cursor(self):
        return self.c.cursor()

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

        self.pool = pycassa.pool.ConnectionPool(keyspace, server_list=servers,
            timeout=90, pool_size=15, *args, **kwargs)

    def get_file(self, name):
        info = self.file_metadata(name)

        if not info:
            return None

        raw = self.get_file_content(name)
        if len(raw) != info['stored_size']:
            raise Exception('Length of retrieved file differs from record: '
                '%d != %d' % (len(raw), info['stored_size']))

        sha1 = hashlib.sha1(raw)
        if sha1.digest() != info['stored_sha1']:
            raise Exception('SHA-1 verification failed.')

        if info['transformation'] in ('zlib', 'gzip'):
            raw = zlib.decompress(raw)
        elif info['transformation'] == 'bzip2':
            raw = bzip2.decompress(raw)

        if info['original_size']:
            if len(raw) != info['original_size']:
                raise Exception('Original file size does not match: '
                    '%d != %d' % (len(raw), info['original_size']))

        if info['original_sha1']:
            sha1 = hashlib.sha1(raw)
            if sha1.digest() != info['original_sha1']:
                raise Exception('Original SHA-1 mismatch.')

        return raw

    def get_file_content(self, key):
        c = self.c.cursor()

        c.execute(b'SELECT i, chunk FROM file_chunks WHERE name = :name '
            b'ORDER BY i ASC', {'name': key})

        chunks = []
        expected = 1
        for i, chunk in c:
            if i != expected:
                raise Exception('Missing file chunk: %d' % i)

            expected += 1
            chunks.append(chunk)

        return b''.join(chunks)

    def store_file(self, filename, content, mtime=-1, transformation=None,
        original_size=None, original_sha1=None):

        transformation = transformation or 'none'
        assert transformation in ('none', 'gzip', 'bzip2', 'zlib')

        if isinstance(mtime, datetime.datetime):
            epoch = datetime.datetime.utcfromtimestamp(0)
            delta = mtime - epoch
            mtime = delta.total_seconds()

        chunk_count = len(content) / DEFAULT_BLOB_CHUNK_SIZE + 1
        sha1 = hashlib.sha1()
        sha1.update(content)

        c = self.c.cursor()
        q_insert_chunk = c.prepare_query(b'INSERT INTO file_chunks '
            b'(name, i, chunk) VALUES (:name, :i, :chunk)')

        q = c.prepare_query(b'INSERT INTO files (name, version, mtime, '
            b'stored_size, stored_sha1, '
            b'transformation, original_size, original_sha1, '
            b'chunk_count, chunk_size) '
            b'VALUES (:name, 1, :mtime, :stored_size, :stored_sha1, '
            b':transformation, :original_size, :original_sha1, '
            b':chunk_count, :chunk_size)')

        offset = 0
        i = 1
        while True:
            b = content[offset:offset + DEFAULT_BLOB_CHUNK_SIZE]

            c.execute_prepared(q_insert_chunk, dict(
                name=filename, i=i, chunk=b))

            if len(b) < DEFAULT_BLOB_CHUNK_SIZE:
                break

            offset += DEFAULT_BLOB_CHUNK_SIZE
            i += 1

        c.execute_prepared(q, dict(
            name=filename, mtime=mtime,
            stored_size=len(content), stored_sha1=sha1.digest(),
            transformation=transformation,
            original_size=original_size, original_sha1=original_sha1,
            chunk_count=chunk_count, chunk_size=DEFAULT_BLOB_CHUNK_SIZE))

        c.close()

    def file_metadata(self, name):
        """Obtain metadata for a stored file.

        Argument is an iterable of file keys whose data to obtain.
        """
        c = self.c.cursor()
        c.execute(b'SELECT * FROM files WHERE name=:name', {'name': name})
        row = c.fetchone()

        if not row:
            return None

        result = {}

        for i, (name, cls) in enumerate(c.name_info):
            result[name] = row[i]

        return result

    def filenames(self):
        """Obtain the keys of all stored files."""
        c = self.c.cursor()
        c.execute(b'SELECT name FROM files')
        for row in c:
            yield row[0]

        c.close()

    def truncate_build_metadata(self):
        """Truncates all derived build metadata.

        This bulk removes all build metadata and should not be performed
        unless you want to reload all derived data!
        """
        c = self.c.cursor()

        for table in BUILDER_TABLES:
            c.execute(b'TRUNCATE %s' % table)

        c.close()

    def drop_build_tables(self):
        c = self.c.cursor()

        for table in BUILDER_TABLES:
            print('Dropping %s' % table)
            c.execute(b'DROP TABLE %s' % table)

        c.close()

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
        c = self.c.cursor()
        c.execute(b'SELECT id, name, category, master FROM builders')
        for row in c:
            yield row

    def builder_categories(self):
        return set(t[2] for t in self.builders())

    def get_builder(self, builder_id):
        """Obtain info about a builder from its ID."""
        c = self.c.cursor()
        c.execute(b'SELECT * FROM builders WHERE id=:id', {'id': builder_id})
        row = c.fetchone()
        data = {}

        for i, (name, cls) in enumerate(c.name_info):
            data[name] = row[i]

        c.close()

        return data

    def builder_ids_in_category(self, category):
        c = self.c.cursor()
        c.execute(b'SELECT id FROM builders WHERE category=:category',
            {'category': category})

        for row in c:
            yield row[0]

        c.close()

    def builder_durations(self):
        c = self.c.cursor()
        c.execute(b'SELECT id, total_duration FROM builder_counters')
        for row in c:
            yield row[0], row[1]

        c.close()

    def builder_counts_in_day(self, day):
        c = self.c.cursor()
        c.execute(b'SELECT id, number FROM builder_daily_counters WHERE '
            b'day=:day', {'day': day})

        for row in c:
            yield row

        c.close()

    def builder_durations_in_day(self, day):
        c = self.c.cursor()
        c.execute(b'SELECT id, duration FROM builder_daily_counters WHERE '
            b'day=:day', {'day': day})

        for row in c:
            yield row

        c.close()

    def builder_counts_in_category(self, category):
        cf = ColumnFamily(self.pool, 'super_counters')
        return self._all_columns_in_supercolumn_column(cf,
            'builder_number_by_category', category, values=True)

    def slaves(self):
        """Obtain basic metadata about all slaves."""
        c = self.c.cursor()
        c.execute(b'SELECT id, name FROM slaves')
        for row in c:
            yield row[0], row[1]

        c.close()

    def slave_id_from_name(self, name):
        c = self.c.cursor()
        c.execute(b'SELECT id FROM slaves WHERE name=:name', {'name': name})
        row = c.fetchone()
        c.close()

        return row[0]

    def build_ids_on_slave(self, slave_id):
        """Obtain all build IDs that were performed on the slave."""
        c = self.c.cursor()
        c.execute(b'SELECT builds FROM slaves WHERE id=:id', {'id': slave_id})
        row = c.fetchone()
        c.close()

        return row[0]

    def build_ids_in_category(self, category):
        """Obtain build IDs having the specified category."""
        c = self.c.cursor()
        c.execute(b'SELECT builds FROM builders WHERE category=:category',
            {'category': category})
        for row in c:
            for build in row[0]:
                yield build

        c.close()

    def build_ids_with_builder_name(self, builder_name):
        c = self.c.cursor()
        c.execute(b'SELECT builds FROM builders WHERE name=:name',
            {'name': builder_name})
        for row in c:
            for build in row[0]:
                yield build

        c.close()

    def build_ids_with_builder_id(self, builder_id):
        c = self.c.cursor()
        c.execute(b'SELECT builds FROM builders WHERE id=:id', {'id':
            builder_id})
        row = c.fetchone()
        for build in row[0]:
            yield build

        c.close()

    def build_from_id(self, build_id):
        """Obtain information about a build from its ID."""
        c = self.c.cursor()
        c.execute(b'SELECT * FROM builds WHERE id=:id', {'id': build_id})
        rows = list(self._cursor_to_dicts(c))
        c.close()

        if not rows:
            return None

        return rows[0]

    def build_durations(self, build_ids=None):
        c = self.c.cursor()
        if build_ids:
            c.execute(b'SELECT id, duration FROM builds WHERE id IN (:ids)',
                {'ids': build_ids})
        else:
            c.execute(b'SELECT id, duration FROM builds')

        for row in c:
            yield row

        c.close()

    def build_durations_with_builder_name(self, builder):
        c = self.c.cursor()
        c.execute(b'SELECT id, duration FROM builds')

        for row in c:
            yield row

        c.close()

    def build_log(self, build_id):
        """Obtain the raw log for a job from its ID."""
        info = self.build_from_id(build_id)
        if not info:
            return None

        if 'log_url' not in info:
            return None

        return self.file_data(info['log_url'])

    def _cursor_to_dicts(self, c):
        names = c.name_info

        for row in c:
            data = {}
            for i, (name, cls) in enumerate(names):
                data[name] = row[i]

            yield data

