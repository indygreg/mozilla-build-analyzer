# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import calendar
import datetime
import fnmatch
import json
import gzip
import httplib
import re
import time
import urllib2

from collections import Counter
from StringIO import StringIO

from pycassa.batch import Mutator
from pycassa.columnfamily import ColumnFamily

from .httputil import ParallelHttpFetcher
from .logparser.jobparser import parse_build_log


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
    (?P<size>[\d-]+)
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

        if d['size'] == '-':
            continue

        t = datetime.datetime.strptime(d['date'], '%d-%b-%Y %H:%M')

        yield d['path'], t, int(d['size'])


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

        fetcher = ParallelHttpFetcher()

        def on_result(request, url):
            if request.status != 200:
                return

            server_state = server_files[url]

            # urllib3 will automagically decompress the files for us.
            data = request.data
            self._connection.store_file(url, data, mtime=server_state[0],
                compression_state='none', compressed_size=server_state[1])
            print('Stored %s' % url)

        for url in sorted(server_files, reverse=True):
            if url not in existing:
                yield 'Will import new file: %s' % url
                fetcher.add_url(url, on_result, (url,))
                continue

            local_meta = existing[url]
            server_meta = server_files[url]

            if local_meta['mtime'] >= server_meta[0]:
                continue

            yield 'Will refresh %s because newer mtime on server' % url
            fetcher.add_url(url, on_result, (url,))
            continue

        fetcher.wait()

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

    def load_missing_logs(self, category=None, builder_pattern=None, before=None, after=None):
        """Loads all logs that aren't currently in storage."""

        if isinstance(after, basestring):
            after = calendar.timegm(time.strptime(after, '%Y-%m-%d'))
        if isinstance(before, basestring):
            before = calendar.timegm(time.strptime(before, '%Y-%m-%d'))

        if not category and not builder_pattern:
            raise Exception('You must limit to a category or builder pattern.')

        builders = list(self._connection.builders())
        builder_names = set(t[1] for t in builders)

        possible_build_ids = set()

        if category:
            possible_build_ids |= \
                set(self._connection.build_ids_in_category(category))

        if builder_pattern:
            for builder_name in builder_names:
                if not fnmatch.fnmatch(builder_name.lower(),
                    builder_pattern.lower()):
                    continue

                possible_build_ids |= \
                    set(self._connection.build_ids_with_builder_name(builder_name))

        yield '%d builds being considered' % len(possible_build_ids)

        to_fetch_count = 0
        fetched_count = 0
        excluded_count = 0

        cf = ColumnFamily(self._pool, 'builds')

        finished_count = [0]
        def on_result(request, build_id, url):
            finished_count[0] += 1
            if request.status != 200:
                return

            data = request.data
            compressed_size = None

            if url.endswith('.gz'):
                s = StringIO(data)
                compressed_size = len(s.getvalue())
                data = gzip.GzipFile(fileobj=s).read()

            now = int(time.time())
            self._connection.store_file(url, data, now,
                compression_state='none', compressed_size=compressed_size)
            cf.insert(build_id, {'log_fetch_status': 'fetched'})
            print('(%d/%d) %s' % (finished_count[0], to_fetch_count, url))

        fetcher = ParallelHttpFetcher()

        for build_id in sorted(possible_build_ids):
            info = self._connection.build_from_id(build_id)
            if not info:
                continue

            if info['log_fetch_status'] != '':
                fetched_count += 1
                continue

            if info['log_url'].startswith('https://pvtbuilds2.dmz.scl3.mozilla.com'):
                excluded_count += 1
                continue

            if after and 'starttime' in info and int(info['starttime']) < after:
                excluded_count += 1
                continue

            if before and 'endtime' in info and int(info['endtime']) > before:
                excluded_count += 1
                continue

            to_fetch_count += 1
            url = info['log_url']
            fetcher.add_url(url, on_result, (build_id, url))

        yield '%d builds have been excluded based on filtering.' % excluded_count
        yield '%d builds have been fetched or do not have a log.' % fetched_count
        yield '%d missing logs will be fetched.' % to_fetch_count

        fetcher.wait()

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

        simple_indices = ColumnFamily(self._pool, 'simple_indices')
        si_batch = simple_indices.batch()

        # We defer counter inserts until then end because Python
        # increments are cheaper than Cassandra increments (hopefully).
        counters = {
            'builder_number': Counter(),
            'builder_duration': Counter(),
            'builder_number_by_day': {},
            'builder_duration_by_day': {},
            'builder_number_by_category': {},
            'builder_duration_by_category': {},
            'builder_number_by_day_and_category': {},
            'builder_duration_by_day_and_category': {},
        }

        existing_filenames = set(self._connection.filenames())

        for build in o:
            self._load_build(batch, i_batch, si_batch, counters, build,
                builders, existing_filenames)

        batch.send()
        i_batch.send()
        si_batch.send()

        cf = ColumnFamily(self._pool, 'counters')
        for builder, count in counters['builder_number'].items():
            cf.add('builder_number', builder, count)

        for builder, count in counters['builder_duration'].items():
            cf.add('builder_duration', builder, count)

        cf = ColumnFamily(self._pool, 'super_counters')
        del counters['builder_number']
        del counters['builder_duration']

        for key, super_columns in counters.items():
            for super_column, counts in super_columns.items():
                for column, count in counts.items():
                    cf.add(key, column, count, super_column)

        return len(o)

    def _load_build(self, batch, i_batch, si_batch, counters, o, builders,
        existing_filenames):

        key = str(o['id'])

        props = o.get('properties', {})
        slave_id = unicode(o['slave_id'])
        master_id = unicode(o['master_id'])
        builder_id = unicode(o['builder_id'])

        i_batch.insert('slave_id_to_build_ids', {slave_id: {key: ''}})
        i_batch.insert('master_id_to_build_ids', {master_id: {key: ''}})
        i_batch.insert('builder_id_to_build_ids', {builder_id: {key: ''}})

        elapsed = o['endtime'] - o['starttime']
        s_elapsed = unicode(elapsed)
        si_batch.insert('build_id_to_duration', {key: s_elapsed})

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

        columns['log_fetch_status'] = ''
        columns['duration'] = elapsed

        # Look for existing log.
        if 'log_url' in columns:
            if columns['log_url'] in existing_filenames:
                columns['log_fetch_status'] = 'fetched'
        else:
            columns['log_fetch_status'] = 'nolog'

        builder_id = columns.get('builder_id')
        if builder_id and builder_id in builders:
            builder = builders[builder_id]
            name, cat  = builder['name'], builder['category']
            columns['builder_category'] = cat
            columns['builder_name'] = name

            i_batch.insert('builder_category_to_build_ids', {cat: {key: ''}})
            i_batch.insert('builder_name_to_build_ids', {name: {key: ''}})

            i_batch.insert('build_duration_by_builder_id', {builder_id: {key:
                s_elapsed}})
            i_batch.insert('build_duration_by_builder_name', {name: {key:
                s_elapsed}})
            i_batch.insert('build_duration_by_builder_category', {cat: {key:
                s_elapsed}})

            counters['builder_number'].update([name])
            counters['builder_duration'][name] += elapsed

            day = datetime.date.fromtimestamp(o['starttime']).isoformat()
            counters['builder_number_by_day'].setdefault(day,
                Counter())[name] += 1
            counters['builder_duration_by_day'].setdefault(day,
                Counter())[name] += elapsed

            counters['builder_number_by_category'].setdefault(cat,
                Counter())[name] += 1
            counters['builder_duration_by_category'].setdefault(cat,
                Counter())[name] += elapsed

            day_cat = '%s.%s' % (day, cat)
            counters['builder_number_by_day_and_category'].setdefault(day_cat,
                Counter())[name] += 1
            counters['builder_duration_by_day_and_category'].setdefault(day_cat,
                Counter())[name] += elapsed

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

    def parse_logs(self, build_ids):
        """Parse the logs for the specified build IDs into storage."""
        # TODO hook up parallel processing.

        OUR_VERSION = '1'
        mut = Mutator(self._pool)
        cf = ColumnFamily(self._pool, 'build_timelines')
        i_cf = ColumnFamily(self._pool, 'indices')
        builds_cf = ColumnFamily(self._pool, 'builds')
        counters = ColumnFamily(self._pool, 'counters')
        super_counters = ColumnFamily(self._pool, 'super_counters')

        for build_id in build_ids:
            info = self._connection.build_from_id(build_id)
            if not info:
                continue

            existing_version = info.get('log_parsing_version')
            if existing_version and existing_version >= OUR_VERSION:
                continue

            if info['log_fetch_status'] != 'fetched':
                continue

            log = self._connection.file_data(info['log_url'])
            if not log:
                continue

            parsed = parse_build_log(log)
            cat = info['builder_category']

            cols = {}
            indices = {}

            for step in parsed.steps:
                start = calendar.timegm(step.start.utctimetuple())
                end = calendar.timegm(step.end.utctimetuple())

                elapsed = end - start
                name = step.name

                cols[start] = {
                    'name': name,
                    'state': step.state,
                    'results': step.results,
                    'start': unicode(start),
                    'end': unicode(end),
                    'elapsed': unicode(elapsed)
                }

                start_date = step.start.date().isoformat()

                indices[name] = {build_id: ''}
                counters.add('build_step_number', name)
                counters.add('build_step_duration', name, elapsed)
                super_counters.add('build_step_number_by_category', name,
                    1, cat)
                super_counters.add('build_step_duration_by_category', name,
                    elapsed, cat)
                super_counters.add('build_step_number_by_day', name, 1,
                    start_date)
                super_counters.add('build_step_duration_by_day', name,
                    elapsed, start_date)

                day_cat = '%s.%s' % (start_date, cat)
                super_counters.add('build_step_number_by_day_and_category',
                    name, 1, day_cat)
                super_counters.add('build_step_duration_by_day_and_category',
                    name, elapsed, day_cat)

            mut.insert(cf, build_id, cols)
            mut.insert(i_cf, 'build_step_name_to_build_ids', indices)
            mut.insert(builds_cf, build_id, {'log_parsing_version': OUR_VERSION})

            yield 'Parsed build %s into %d steps.' % (build_id,
                len(parsed.steps))

        mut.send()
