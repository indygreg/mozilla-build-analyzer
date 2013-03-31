# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import fnmatch
import json
import gzip
import httplib
import multiprocessing
import re
import time
import urllib2

from StringIO import StringIO

from pycassa.columnfamily import ColumnFamily


# Where public build data is available from. Feed into strftime for the day you
# want.
BUILD_DATA_PREFIX = 'http://builddata.pub.build.mozilla.org/buildjson/'
BUILD_DATA_URL = BUILD_DATA_PREFIX + 'builds-%Y-%m-%d.js.gz'
BUILD_DATA_HOST = 'builddata.pub.build.mozilla.org'
BUILD_DATA_PORT = 80

RE_BUILD_LISTING_ENTRY = re.compile(r'''
    ^<a\shref="(?P<path>[^"]+)">[^<]+<\/a>
    \s+
    (?P<date>\d{2}-[^-]+-\d{4}\s\d{2}:\d{2})
    \s+
    (?P<size>\d+)
    $''', re.VERBOSE)

def available_build_files():
    """Obtain info of available build data files on the server."""

    # The HTML is simple enough we don't bother with a regular parser.
    html = urllib2.urlopen(BUILD_DATA_PREFIX).read()

    for line in html.splitlines():
        if not line.startswith('<a'):
            continue

        match = RE_BUILD_LISTING_ENTRY.match(line)
        assert match

        d = match.groupdict()

        if d['path'].endswith('.tmp'):
            continue

        t = time.strptime(d['date'], '%d-%b-%Y %H:%M')

        yield d['path'], time.mktime(t), int(d['size'])


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

    def __init__(self, connection):
        """Instantiate against a Cassandra Connection Pool."""

        self._connection = connection
        self._pool = connection.pool

    def synchronize_build_files(self):
        """Synchronize our set of raw build files with what's on the server."""
        server_files = {BUILD_DATA_PREFIX + t[0]: (t[1], t[2])
            for t in available_build_files()}

        existing = {}
        for url in server_files:
            existing.update(self._connection.file_metadata([url]))
        populate = set()

        for url in server_files:
            if url not in existing:
                populate.add(url)
                continue

            local_meta = existing[url]
            server_meta = server_files[url]

            if local_meta['mtime'] < server_meta[0]:
                yield 'Will refresh %s because newer mtime on server' % url
                populate.add(url)
                continue

            if local_meta['size'] != server_meta[1]:
                yield 'Will refresh %s because size changed' % url
                populate.add(url)
                continue

        # We reuse the connection take advantage of slow start, etc.
        conn = httplib.HTTPConnection(BUILD_DATA_HOST, BUILD_DATA_PORT)

        for url in sorted(populate, reverse=True):
            yield 'Adding %s' % url
            conn.request('GET', url)
            response = conn.getresponse()
            if response.status != 200:
                raise Exception('Error fetching %s: %d' % (url,
                    response.status))

            data = response.read()
            compression = None
            if url.endswith('.gz'):
                compression = 'gzip'

            self._connection.store_file(url, data, mtime=server_files[url][0],
                compression=compression)

    def load_build_metadata(self, url):
        """Loads build metadata from JSON in a URL."""
        file_info = self._connection.file_metadata([url])

        if url not in file_info:
            raise Exception('Build file does not exist in local storage: %s' %
                url)

        file_info = file_info[url]
        raw = self._connection.file_data(url)
        if not raw:
            raise Exception('No data appears to be stored: %s' % url)

        obj = json.loads(raw, encoding='utf-8')
        return self.load_builds_json(obj)

    def load_builds_from_day(self, t):
        url = time.strftime(BUILD_DATA_URL, time.gmtime(t))
        return self.load_build_metadata(url)

    def load_builds_json(self, obj):
        yield 'Loaded %d slaves' % self.load_slaves(obj['slaves'])
        yield 'Loaded %d masters' % self.load_masters(obj['masters'])
        yield 'Loaded %d builders' % self.load_builders(obj['builders'])
        yield 'Loaded %d builds' % self.load_builds(obj['builds'],
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
        with cf.batch() as batch:
            for slave_id, name in o.items():
                batch.insert(slave_id, {'name': name})

        return len(o)

    def load_masters(self, o):
        cf = ColumnFamily(self._pool, 'masters')

        with cf.batch() as batch:
            for master_id, info in o.items():
                batch.insert(master_id, info)

        return len(o)

    def load_builds(self, o, builders):
        cf = ColumnFamily(self._pool, 'builds')
        batch = cf.batch()

        indices = ColumnFamily(self._pool, 'indices')
        i_batch = indices.batch()

        for build in o:
            self._load_build(batch, i_batch, build, builders)

        batch.send()
        i_batch.send()

        return len(o)

    def _load_build(self, batch, i_batch, o, builders):
        key = str(o['id'])

        props = o.get('properties', {})
        slave_id = unicode(o['slave_id'])
        master_id = unicode(o['master_id'])
        builder_id = unicode(o['builder_id'])

        i_batch.insert('slave_id_to_build_ids', {slave_id: {key: ''}})
        i_batch.insert('master_id_to_build_ids', {master_id: {key: ''}})
        i_batch.insert('builder_id_build_ids', {builder_id: {key: ''}})

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
            builder = builders[builder_id]
            columns['builder_category'] = builder['category']
            columns['builder_name'] = builder['name']

            i_batch.insert('builder_category_to_build_ids',
                {builder['category']: {key: ''}})

        batch.insert(key, columns)

    def load_builders(self, o):
        cf = ColumnFamily(self._pool, 'builders')
        indices = ColumnFamily(self._pool, 'indices')

        batch = cf.batch()
        i_batch = indices.batch()

        for builder_id, params in o.items():
            cat = params['category']
            columns = {
                'category': cat,
                'master': unicode(params['master_id']),
                'name': params['name'],
            }

            batch.insert(builder_id, columns)
            i_batch.insert('builder_category_to_builder_ids',
                {cat: {builder_id: ''}})
            i_batch.insert('master_id_to_slave_ids', {columns['master']: {
                builder_id: ''}})

            if len(params['slaves']):
                i_batch.insert('builder_id_to_slave_ids', {builder_id: {
                    unicode(slave_id): '' for slave_id in params['slaves']}})

        batch.send()
        i_batch.send()

        return len(o)

