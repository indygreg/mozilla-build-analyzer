# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import fnmatch
import json
import gzip
import multiprocessing
import time
import urllib2

from pycassa.columnfamily import ColumnFamily

from StringIO import StringIO


# Where public build data is available from. Feed into strftime for the day you
# want.
BUILD_DATA_URL = 'http://builddata.pub.build.mozilla.org/buildjson/builds-%Y-%m-%d.js.gz'


def get_daily_data(t=None):
    """Fetch the build data for the day of the specified time.

    Returns an object containing the raw JSON.
    """

    url = time.strftime(BUILD_DATA_URL, time.gmtime(t))
    io = urllib2.urlopen(url)
    s = StringIO(io.read())
    io.close()

    return json.load(gzip.GzipFile(fileobj=s), encoding='utf-8')


def fetch_log(params):
    job, url = params

    try:
        io = urllib2.urlopen(url)
        return True, job, io.read()
    except KeyboardInterrupt:
        return
    except Exception as e:
        print(e)
        return False, job, None


def fetch_logs(cf, log_cf, urls):
    pool = multiprocessing.Pool(processes=4)

    entries = [(k, v) for k, v in urls.items()]

    remaining = len(entries)

    for success, job, data in pool.imap(fetch_log, entries):
        remaining -= 1

        if success:
            now = unicode(int(time.time()))
            cf.insert(unicode(job), {'log_fetch_time': now})
            log_cf.insert(unicode(job), {
                'fetch_time': now,
                'log': data,
            })
            yield '%d Stored log for %s' % (remaining, job)


class DataLoader(object):
    """Load buildbot data into Cassandra."""

    def __init__(self, pool):
        """Instantiate against a Cassandra Connection Pool."""

        self._pool = pool

    def load_builds_from_day(self, t):
        obj = get_daily_data(t)

        yield 'Loaded %d slaves' % self.load_slaves(obj['slaves'])
        yield 'Loaded %d masters' % self.load_masters(obj['masters'])
        yield 'Loaded %d builders' % self.load_builders(obj['builders'])
        yield 'Loaded %d jobs' % self.load_builds(obj['builds'],
            obj['builders'])

    def load_missing_logs(self, builder_pattern=None):
        """Loads all logs that aren't currently in storage."""
        missing_urls = {}

        cf = ColumnFamily(self._pool, 'jobs')
        columns = ('buildername', 'log_url', 'log_fetch_time')
        for key, cols in cf.get_range(columns=columns):
            if 'log_url' not in cols:
                continue

            if 'log_fetch_time' in cols:
                continue

            if builder_pattern and ('buildername' not in cols or \
                not fnmatch.fnmatch(cols['buildername'].lower(),
                    builder_pattern.lower())):
                continue

            missing_urls[key] = cols['log_url']

        yield '%d missing logs will be fetched.' % len(missing_urls)
        log_cf = ColumnFamily(self._pool, 'raw_job_logs')
        for result in fetch_logs(cf, log_cf, missing_urls):
            yield result

    def load_slaves(self, o):
        cf = ColumnFamily(self._pool, 'slaves')
        batch = cf.batch()

        for slave_id, name in o.items():
            batch.insert(name, {'id': slave_id})

        return len(o)

    def load_masters(self, o):
        cf = ColumnFamily(self._pool, 'masters')
        batch = cf.batch()

        for master_id, info in o.items():
            key = info['name']
            columns = {k: v for k, v in info.items() if k != 'name'}
            columns['id'] = master_id

            batch.insert(key, columns)

        return len(o)

    def load_builds(self, o, builders):
        cf = ColumnFamily(self._pool, 'jobs')
        batch = cf.batch()

        slave_jobs = ColumnFamily(self._pool, 'slave_jobs')
        sjb = slave_jobs.batch()

        for job in o:
            self._load_job(batch, sjb, job, builders)

        return len(o)

    def _load_job(self, batch, slave_jobs_batch, o, builders):
        key = str(o['id'])

        props = o.get('properties', {})
        slavename = props.get('slavename')
        if slavename:
            slave_jobs_batch.insert(slavename, {
                long(o['id']): key
            })

        columns = {}
        for k, v in o.items():
            if isinstance(v, int):
                columns[k] = unicode(v)
                continue

            if isinstance(v, basestring):
                columns[k] = v
                continue

            if v is None:
                continue

            # TODO handle later.
            if k == 'request_ids':
                continue

            if k == 'properties':
                for k2, v2 in v.items():
                    if isinstance(v2, int):
                        columns[k2] = unicode(v2)
                        continue

                    if isinstance(v2, basestring):
                        columns[k2] = v2
                        continue

                    # TODO handle non-scalar fields.

                continue

            raise Exception('Unknown non-simple field: %s %s' % (k, v))

        builder_id = columns.get('builder_id')
        if builder_id and builder_id in builders:
            columns['builder_category'] = builders[builder_id]['category']
            columns['builder_name'] = builders[builder_id]['name']

        batch.insert(key, columns)

    def load_builders(self, o):
        cf = ColumnFamily(self._pool, 'builders')
        batch = cf.batch()

        for builder_id, params in o.items():
            columns = {
                'category': params['category'],
                'master': unicode(params['master_id']),
                'name': params['name'],
                # TODO slaves
            }

            batch.insert(builder_id, columns)

        return len(o)

