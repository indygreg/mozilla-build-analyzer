# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import gzip
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
    'slaves': {
        'comment': 'Holds information about slaves that run jobs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'slave_jobs': {
        'comment': 'Maps slave names to job IDs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': LONG_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'masters': {
        'comment': 'Describes buildbot masters.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'builders': {
        'comment': 'Information about different job types (builders).',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'jobs': {
        'comment': 'Information about individual build jobs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'UTF8Type',
    },
    'raw_job_logs': {
        'comment': 'Raw job logs.',
        'key_validation_class': 'UTF8Type',
        'comparator_type': UTF8_TYPE,
        'default_validation_class': 'BytesType',
    },
}

COLUMN_TYPES = {}

INDEXES = {
    'jobs': {
        'builder_category': ('UTF8Type', KEYS_INDEX, 'category_index'),
    },
}


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

            for column, index in INDEXES.get(name, {}).items():
                e = existing_metadata.get(column)
                if e and hasattr(e, 'index_name'):
                    continue

                manager.create_index(keyspace, name, column, *index)

        self.pool = pycassa.pool.ConnectionPool(keyspace, server_list=servers,
            *args, **kwargs)

    def builders(self):
        """Obtain info about all builders."""
        cf = ColumnFamily(self.pool, 'builders')
        for key, cols in cf.get_range(columns=['category', 'master', 'name']):
            yield key, cols['name'], cols['category'], cols['master']

    def slaves(self):
        """Obtain basic metadata about all slaves."""

        cf = ColumnFamily(self.pool, 'slaves')

        for key, cols in cf.get_range(columns=['id']):
            yield key, cols['id']

    def job_ids_on_slave(self, name):
        """Obtain all job IDs that were performed on named slave."""
        cf = ColumnFamily(self.pool, 'slave_jobs')

        try:
            cols = cf.get(name)
        except NotFoundException:
            return []

        return cols.values()

    def jobs_on_slave(self, name):
        """Obtain information about all jobs on a specific slave."""

        ids = self.job_ids_on_slave(name)
        cf = ColumnFamily(self.pool, 'jobs')
        result = cf.multiget(ids, columns=self.basic_job_columns)

        for key, cols in result.items():
            yield key, cols

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

    @property
    def basic_job_columns(self):
        return (
            'basedir',
            'branch',
            'build_url',
            'builder_id',
            'buildername',
            'buildid',
            'buildnumber',
            'builduid',
            'endtime',
            'id',
            'log_url',
            'master',
            'master_id',
            'platform',
            'product',
            'project',
            'reason',
            'repo_path',
            'repository',
            'requesttime',
            'result',
            'revision',
            'scheduler',
            'script_repo_revision',
            'slave_id',
            'slavebuilddir',
            'slavename',
            'stage',
            'starttime',
        )
