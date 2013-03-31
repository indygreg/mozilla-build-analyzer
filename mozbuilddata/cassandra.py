# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import pycassa

from pycassa.columnfamily import ColumnFamily

from pycassa.system_manager import (
    BYTES_TYPE,
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

COLUMN_TYPES = {
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

            # TODO only alter columns if they've changed.
            for column, column_type in COLUMN_TYPES.get(name, {}).items():
                manager.alter_column(keyspace, name, column, column_type)

        self.pool = pycassa.pool.ConnectionPool(keyspace, server_list=servers,
            *args, **kwargs)

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
