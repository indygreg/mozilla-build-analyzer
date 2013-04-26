# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import calendar
import datetime
import fnmatch
import json
import gzip
import hashlib
import httplib
import re
import time
import urllib2
import zlib

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

    def synchronize_build_files(self):
        """Synchronize our set of raw build files with what's on the server."""
        server_files = {BUILD_DATA_PREFIX + t[0]: (t[1], t[2])
            for t in available_build_files()}

        existing = {}
        for url in server_files:
            meta = self._connection.file_metadata(url)
            if meta:
                existing[url] = meta

        fetcher = ParallelHttpFetcher()

        def on_result(request, url):
            if request.status != 200:
                return

            server_state = server_files[url]

            # urllib3 will automagically decompress the files for us.
            data = request.data
            compressed = zlib.compress(data)
            sha1 = hashlib.sha1(data)

            self._connection.store_file(url, compressed, mtime=server_state[0],
                transformation='zlib', original_size=len(data),
                original_sha1=sha1.digest())
            print('Stored %s' % url)

        for url in sorted(server_files, reverse=True):
            if url not in existing:
                yield 'Will import new file: %s' % url
                fetcher.add_url(url, on_result, (url,))
                continue

            local_meta = existing[url]
            server_meta = server_files[url]

            local_mtime = datetime.datetime.utcfromtimestamp(local_meta['mtime'])
            if local_mtime >= server_meta[0]:
                continue

            yield 'Will refresh %s because newer mtime on server' % url
            fetcher.add_url(url, on_result, (url,))
            continue

        fetcher.wait()

    def load_build_metadata(self, url):
        """Loads build metadata from JSON in a URL."""
        raw = self._connection.get_file(url)
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

    def load_missing_logs(self, category=None, builder_pattern=None, after=None):
        """Loads all logs that aren't currently in storage."""

        if isinstance(after, basestring):
            after = calendar.timegm(time.strptime(after, '%Y-%m-%d'))

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

            to_fetch_count += 1
            url = info['log_url']
            fetcher.add_url(url, on_result, (build_id, url))

        yield '%d builds have been excluded based on filtering.' % excluded_count
        yield '%d builds have been fetched or do not have a log.' % fetched_count
        yield '%d missing logs will be fetched.' % to_fetch_count

        fetcher.wait()

    def load_slaves(self, o):
        c = self._connection.cursor()
        q = c.prepare_query(b'INSERT INTO slaves (id, name) VALUES (:id, :name)')
        for slave_id, name in o.items():
            c.execute_prepared(q, dict(id=int(slave_id), name=name))

        c.close()

        return len(o)

    def load_masters(self, o):
        c = self._connection.cursor()
        q = c.prepare_query(b'INSERT INTO masters (id, name, url) '
            b'VALUES (:id, :name, :url)')

        for master_id, info in o.items():
            c.execute_prepared(q, dict(id=int(master_id), name=info['name'],
                url=info['url']))

        c.close()

        return len(o)

    def load_builders(self, o):
        c = self._connection.cursor()
        q1 = c.prepare_query(b'INSERT INTO builders (id, name, category, master) '
            b'VALUES (:id, :name, :category, :master)')
        q2 = c.prepare_query(b'UPDATE builders SET slaves = slaves + :slaves '
            b'WHERE id=:id')
        q3 = c.prepare_query(b'UPDATE masters SET builders = builders + '
            b':builders WHERE id=:id')

        for builder_id, params in o.items():
            builder_id = int(builder_id)
            master_id = int(params['master_id'])

            c.execute_prepared(q1, dict(id=builder_id, name=params['name'],
                category=params['category'], master=master_id))

            if params['slaves']:
                c.execute_prepared(q2, dict(id=builder_id,
                    slaves=params['slaves']))

            c.execute_prepared(q3, dict(id=master_id, builders=[builder_id]))

        c.close()
        return len(o)

    BUILD_PROPERTIES = dict(
        builder_name='buildername',
        app_name='appName',
        app_version='appVersion',
        base_dir='basedir',
        branch='branch',
        build_dir='builddir',
        build_filename='build_filename',
        build_id='buildid',
        build_uid='builduid',
        build_url='build_url',
        comments='comments',
        exe_dir='exedir',
        file_path='filepath',
        foopy_type='foopy_type',
        forced_clobber='forced_clobber',
        got_revisions='got_revisions',
        hash_type='hashType',
        http_port='http_port',
        hostutils_filename='hostutils_filename',
        hostutils_url='hostutils_url',
        js_shell_url='jsshellUrl',
        log_url='log_url',
        master_url='master',
        num_ctors='num_ctors',
        package_filename='packageFilename',
        package_hash='packageHash',
        package_size='packageSize',
        package_url='packageUrl',
        periodic_clobber='periodic_clobber',
        pgo_build='pgo_build',
        platform='platform',
        product='product',
        project='project',
        purge_actual='purge_actual',
        purge_target='purge_target',
        purged_clobber='purged_clobber',
        reason='reason',
        revision='revision',
        request_ids='request_ids',
        repo_path='repo_path',
        repository='repository',
        robocop_filename='robocop_filename',
        robocop_url='robocop_url',
        scheduler='scheduler',
        slave_build_dir='slavebuilddir',
        slave_name='slavename',
        sourcestamp='sourcestamp',
        ssl_port='ssl_port',
        stage_platform='stage_platform',
        symbols_url='symbolsUrl',
        tests_filename='tests_filename',
        tests_url='testsUrl',
        tools_dir='toolsdir',
        who='who',
    )

    def load_builds(self, o, builders):
        c = self._connection.cursor()

        q_add_to_builder = c.prepare_query(b'UPDATE builders SET '
            b'builds = builds + :build_ids WHERE id=:id')

        q_add_to_slave = c.prepare_query(b'UPDATE slaves SET '
            b'builds = builds + :build_ids WHERE id=:id')

        q_add_events_to_slave = c.prepare_query(b'UPDATE slaves SET '
            b'build_events[:start] = :start_value, '
            b'build_events[:end] = :end_value '
            b'WHERE id=:id')

        q_add_builder_count = c.prepare_query(b'UPDATE builder_counters '
            b'SET total_number = total_number + 1 WHERE id=:id')

        q_add_builder_duration = c.prepare_query(b'UPDATE builder_counters '
            b'SET total_duration = total_duration + :duration WHERE id=:id')

        q_add_daily_builder_count = c.prepare_query(
            b'UPDATE builder_daily_counters SET number = number + 1 '
            b'WHERE id=:id and day=:day')

        q_add_daily_builder_duration = c.prepare_query(
            b'UPDATE builder_daily_counters SET duration = duration + :d '
            b'WHERE id=:id and day=:day')

        q_add_category_count = c.prepare_query(
            b'UPDATE builder_category_counters SET number = number + 1 '
            b'WHERE category = :category')

        q_add_category_duration = c.prepare_query(
            b'UPDATE builder_category_counters SET duration = duration + :d '
            b'WHERE category = :category')

        q_add_daily_category_count = c.prepare_query(
            b'UPDATE builder_category_daily_counters SET number = number + 1 '
            b'WHERE category = :category AND day = :day')

        q_add_daily_category_duration = c.prepare_query(
            b'UPDATE builder_category_daily_counters '
            b'SET duration = duration + :d '
            b'WHERE category = :category AND day = :day')


        epoch = datetime.date.fromtimestamp(0)

        for build in o:
            props = build['properties']
            bid = build['id']
            builder_id = build['builder_id']
            slave_id = build['slave_id']
            builder = builders[unicode(builder_id)]

            start_day = datetime.date.fromtimestamp(build['starttime'])
            start_day_ts = (start_day - epoch).total_seconds()
            duration = build['endtime'] - build['starttime']

            c.execute_prepared(q_add_to_builder,
                dict(id=builder_id, build_ids=[bid]))

            c.execute_prepared(q_add_to_slave,
                dict(id=slave_id, build_ids=[bid]))

            c.execute_prepared(q_add_events_to_slave,
                dict(id=slave_id,
                    start=build['starttime'], start_value='start-%d' % bid,
                    end=build['endtime'], end_value='end-%d' % bid))

            c.execute_prepared(q_add_builder_count, dict(id=builder_id))
            c.execute_prepared(q_add_builder_duration, dict(
                id=builder_id, duration=duration))

            c.execute_prepared(q_add_daily_builder_count, dict(
                id=builder_id, day=start_day_ts))

            c.execute_prepared(q_add_daily_builder_duration, dict(
                id=builder_id, day=start_day_ts, d=duration))

            c.execute_prepared(q_add_category_count, dict(
                category=builder['category']))

            c.execute_prepared(q_add_category_duration, dict(
                category=builder['category'], d=duration))

            c.execute_prepared(q_add_daily_category_count, dict(
                category=builder['category'], day=start_day_ts))

            c.execute_prepared(q_add_daily_category_duration, dict(
                category=builder['category'], day=start_day_ts,
                d=duration))

            p = dict(
                id=bid,
                builder_id=builder_id,
                builder_name=builder['name'],
                builder_category=builder['category'],
                build_number=build['buildnumber'],
                master_id=build['master_id'],
                slave_id=slave_id,
                request_time=build['requesttime'],
                start_time=build['starttime'],
                end_time=build['endtime'],
                duration=duration,
                result=build['result'],
            )

            v = self.BUILD_PROPERTIES.values()
            for k in props:
                if k not in v:
                    print(k)

        return len(o)

        cf = ColumnFamily(self._pool, 'builds')
        batch = cf.batch()

        indices = ColumnFamily(self._pool, 'indices')
        i_batch = indices.batch()

        simple_indices = ColumnFamily(self._pool, 'simple_indices')
        si_batch = simple_indices.batch()

        existing_filenames = set(self._connection.filenames())

        for build in o:
            self._load_build(batch, i_batch, si_batch, counters, build,
                builders, existing_filenames)

        batch.send()
        i_batch.send()
        si_batch.send()

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
