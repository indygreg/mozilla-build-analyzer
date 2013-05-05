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
import pytz
import re
import time
import urllib2
import zlib

from threading import Thread

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

TZ_MV = pytz.timezone('America/Los_Angeles')


def make_chunks(data, n):
    chunk_size = len(data) / n + 1
    chunks = []
    current = []

    for i, e in enumerate(data):
        current.append(e)

        if i and i % chunk_size == 0:
            chunks.append(current)
            current = []

    if current:
        chunks.append(current)

    return chunks


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
            yield 'Build JSON not available: %s' % url
            return

        obj = json.loads(raw, encoding='utf-8')
        for msg in self.load_builds_json(obj):
            yield msg

    def load_builds_from_day(self, day):
        url = day.strftime(BUILD_DATA_URL)
        return self.load_build_metadata(url)

    def load_builds_json(self, obj):
        yield 'Loaded %d masters' % self.load_masters(obj['masters'])
        yield 'Loaded %d slaves' % self.load_slaves(obj['slaves'])
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

        finished_count = [0]
        def on_result(request, build_id, url):
            finished_count[0] += 1
            if request.status != 200:
                return

            data = request.data
            compressed_size = None

            transformation = None

            if url.endswith('.gz'):
                transformation = 'gzip'

            now = int(time.time())

            self._connection.store_file(url, data, mtime=now,
                transformation=transformation)
            print('(%d/%d) %s' % (finished_count[0], to_fetch_count, url))

        fetcher = ParallelHttpFetcher()

        for build_id in sorted(possible_build_ids):
            info = self._connection.builds.get_build(build_id)
            if not info or 'log_url' not in info:
                excluded_count += 1
                continue

            url = info['log_url']
            if not url:
                excluded_count += 1
                continue

            if url.startswith('https://pvtbuilds2.dmz.scl3.mozilla.com'):
                excluded_count += 1
                continue

            if after and 'starttime' in info and int(info['starttime']) < after:
                excluded_count += 1
                continue

            existing = self._connection.file_metadata(url)
            if existing:
                fetched_count += 1
                continue

            to_fetch_count += 1
            fetcher.add_url(url, on_result, (build_id, url))

        yield '%d builds have been excluded based on filtering.' % excluded_count
        yield '%d builds have been fetched or do not have a log.' % fetched_count
        yield '%d missing logs will be fetched.' % to_fetch_count

        fetcher.wait()

    def load_slaves(self, o):
        with self._connection.cursor() as c:
            q = c.prepare_query(b'INSERT INTO slaves (id, name) VALUES (:id, :name)')
            for slave_id, name in o.items():
                c.execute_prepared(q, dict(id=int(slave_id), name=name))

        return len(o)

    def load_masters(self, o):
        with self._connection.cursor() as c:
            q = c.prepare_query(b'INSERT INTO masters (id, name, url) '
                b'VALUES (:id, :name, :url)')

            for master_id, info in o.items():
                c.execute_prepared(q, dict(id=int(master_id), name=info['name'],
                    url=info['url']))

        return len(o)

    def load_builders(self, o, workers=4):
        chunks = make_chunks(o.items(), workers)

        threads = []
        for chunk in chunks:
            t = Thread(target=DataLoader._load_builders_worker,
                args=(self, dict(chunk)))
            t.start()
            threads.append(t)

        # TODO handle KeyboardInterrupt
        for t in threads:
            t.join()

        return len(o)

    @staticmethod
    def _load_builders_worker(self, o):
        # TODO this can deadlock if # workers > # connections.
        with self._connection.cursor() as c:
            q = c.prepare_query(b'''
                BEGIN BATCH
                INSERT INTO builders (id, name, category, master)
                    VALUES (:builder_id, :name, :category, :master_id);
                UPDATE builder_categories SET builders = builders + :builders
                    WHERE category=:category;
                UPDATE builders SET slaves = slaves + :slaves
                    WHERE id=:builder_id;
                UPDATE masters SET builders = builders + :builders
                    WHERE id=:master_id;
                APPLY BATCH
            ''')

            for builder_id, params in o.items():
                builder_id = int(builder_id)
                master_id = int(params['master_id'])

                c.execute_prepared(q, dict(
                    builder_id=builder_id,
                    name=params['name'],
                    category=params['category'],
                    master_id=master_id,
                    slaves=params['slaves'],
                    builders=[builder_id],
                ))

        return len(o)

    # Maps properties from builds into column names.
    BUILD_PROPERTIES = dict(
        buildername='builder_name',
        app='app',
        appName='app_name',
        appVersion='app_version',
        basedir='base_dir',
        branch='branch',
        build_filename='build_filename',
        build_url='build_url',
        builddir='build_dir',
        buildid='build_id',
        buildnumber=('build_number', int),
        builduid='build_uid',
        comments='comments',
        comm_revision='comm_revision',
        compare_locales_revision='compare_locales_revision',
        completeMarFilename='complete_mar_filename',
        completeMarHash='complete_mar_hash',
        completeMarSize=('complete_mar_size', int),
        completeMarUrl='complete_mar_url',
        completesnippetFilename='complete_snippet_filename',
        configFile='config_file',
        configRevision='config_revision',
        en_revision='en_revision',
        exedir='exe_dir',
        exepath='exe_path',
        fennec_ids_filename='fennec_ids_filename',
        fennec_ids_url='fennec_ids_url',
        filename='filename',
        filepath='file_path',
        fileURL='file_url',
        foopy_type='foopy_type',
        forced_clobber=('forced_clobber', bool),
        fx_revision='fx_revision',
        gaia_revision='gaia_revision',
        gecko_revision='gecko_revision',
        got_revision='got_revision',
        hashType='hash_type',
        http_port=('http_port', int),
        hostutils_filename='hostutils_filename',
        hostutils_url='hostutils_url',
        inipath='ini_path',
        installerHash='installer_hash',
        installerFilename='installer_filename',
        installerSize=('installer_size', int),
        jsshellUrl='js_shell_url',
        l10n_revision='l10n_revision',
        locale='locale',
        log_url='log_url',
        master='master_url',
        mozmillVirtualenvSetup='mozmill_virtualenv_setup',
        moz_revision='moz_revision',
        num_ctors=('num_ctors', int),
        nightly_build=('nightly_build', bool),
        packageFilename='package_filename',
        packageHash='package_hash',
        packageSize=('package_size', int),
        packageUrl='package_url',
        partialMarFilename='partial_mar_filename',
        partialMarHash='partial_mar_hash',
        partialMarSize=('partial_mar_size', int),
        partialMarUrl='partial_mar_url',
        partialsnippetFilename='partial_snippet_filename',
        periodic_clobber=('periodic_clobber', bool),
        pgo_build=('pgo_build', bool),
        platform='platform',
        previousMarFilename='previous_mar_filename',
        previous_buildid='previous_build_id',
        previous_inipath='previous_ini_path',
        product='product',
        products='products',
        project='project',
        purge_actual='purge_actual',
        purge_target='purge_target',
        purged_clobber=('purged_clobber', bool),
        reason='reason',
        release_config='release_config',
        release_tag='release_tag',
        remoteProcessName='remote_process_name',
        revision='revision',
        request_ids='request_ids',
        repo_path='repo_path',
        repository='repository',
        robocopApkUrl='robocop_apk_url',
        robocop_filename='robocop_filename',
        robocop_url='robocop_url',
        scheduler='scheduler',
        script_repo_revision='script_repo_revision',
        slavebuilddir='slave_build_dir',
        slavename='slave_name',
        slowTests=('slow_tests', bool),
        sourcestamp='sourcestamp',
        ssl_port=('ssl_port', int),
        stage_platform='stage_platform',
        sut_ip='sut_ip',
        symbolsFile='symbols_filename',
        symbols_filename='symbols_filename',
        symbolsUrl='symbols_url',
        symbols_url='symbols_url',
        tests_filename='tests_filename',
        testsUrl='tests_url',
        tests_url='tests_url',
        toolsdir='tools_dir',
        tools_revision='tools_revision',
        tree='tree',
        unsignedApkUrl='unsigned_apk_url',
        upload_host='upload_host',
        upload_sshkey='upload_ssh_key',
        upload_user='upload_user',
        version='version',
        vsize=('vsize', int),
        who='who',
    )

    def load_builds(self, o, builders, workers=4):
        chunks = make_chunks(o, workers)

        state = [0, len(o)]
        threads = []
        for chunk in chunks:
            t = Thread(target=DataLoader._build_loader_worker,
                args=(self, chunk, builders, state))
            t.start()
            threads.append(t)

        # TODO handle keyboardinterrupt
        for t in threads:
            t.join()

        return len(o)

    @staticmethod
    def _build_loader_worker(self, o, builders, state):
        # TODO this can deadlock if # workers > # connections.
        with self._connection.cursor() as c:
            self._load_builds(o, builders, c, state)

    def _load_builds(self, o, builders, c, state):
        q_derived = c.prepare_query(b'''
            BEGIN BATCH
            UPDATE builders SET builds = builds + :build_ids WHERE
                id=:builder_id;
            UPDATE builder_categories SET builds = builds + :build_ids
                WHERE category=:category;
            UPDATE slaves SET builds = builds + :build_ids WHERE id=:slave_id;
            UPDATE masters SET builds = builds + :build_ids WHERE
                id=:master_id;
            APPLY BATCH
        ''')

        # This should be part of the above. However, the Cassandra server is
        # choking on it for some reason.
        q_update_slave_events = c.prepare_query(b'UPDATE slaves SET '
            b'build_events[:start] = :start_value, '
            b'build_events[:end] = :end_value '
            b'WHERE id=:slave_id')

        q_update_builder_categories = c.prepare_query(b'''
            UPDATE builder_categories SET
                build_durations[:build_id] = :duration
                WHERE category = :category;
            ''')

        q_counters = c.prepare_query(b'''
            BEGIN COUNTER BATCH
            UPDATE builder_counters SET
                total_number = total_number + 1,
                total_duration = total_duration + :duration
                WHERE id=:builder_id;

            UPDATE builder_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE id=:builder_id AND day=:utc_day AND is_utc=true;

            UPDATE builder_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE id=:builder_id AND day=:mv_day AND is_utc=false;

            UPDATE builder_category_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE category=:category;

            UPDATE builder_category_monthly_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE category=:category AND month=:utc_month;

            UPDATE builder_category_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE category=:category AND day=:utc_day AND is_utc=true;

            UPDATE builder_category_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE category=:category AND day=:mv_day AND is_utc=false;

            UPDATE build_monthly_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE month=:utc_month;

            APPLY BATCH
            ''')

        existing_rows = {}
        build_ids = [build['id'] for build in o]
        for row in self._connection.builds.get_builds(build_ids):
            existing_rows[row['id']] = row

        epoch = datetime.date.fromtimestamp(0)

        for build in o:
            state[0] += 1

            props = build['properties']
            bid = build['id']
            builder_id = build['builder_id']
            slave_id = build['slave_id']
            builder = builders[unicode(builder_id)]

            utc_start = datetime.datetime.utcfromtimestamp(build['starttime'])
            mv_start = datetime.datetime.fromtimestamp(build['starttime'],
                TZ_MV)
            start_day = datetime.date.fromtimestamp(build['starttime'])
            start_day_ts = (start_day - epoch).total_seconds()
            duration = build['endtime'] - build['starttime']

            existing = existing_rows.get(bid, None)

            if existing:
                print('%d/%d Updating %d' % (state[0], state[1], bid))
            if not existing:
                print('%d/%d Adding %d' % (state[0], state[1], bid))

                prepared_params = dict(
                    build_id=bid,
                    build_ids=[bid],
                    builder_id=builder_id,
                    slave_id=slave_id,
                    master_id=build['master_id'],
                    duration=duration,
                    utc_month=utc_start.date().strftime('%Y-%m'),
                    utc_day=utc_start.date().isoformat(),
                    mv_day=mv_start.date().isoformat(),
                    category=builder['category'],
                    start=build['starttime'], start_value='start-%d' % bid,
                    end=build['endtime'], end_value='end-%d' % bid
                )

                c.execute_prepared(q_derived, prepared_params)
                c.execute_prepared(q_update_slave_events, prepared_params)
                c.execute_prepared(q_update_builder_categories,
                    prepared_params)
                c.execute_prepared(q_counters, prepared_params)

            p = dict(
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

            if not p['request_time']:
                print('Build does not have defined request time: %d' % bid)
                p['request_time'] = 0

            for k in props:
                # This is almost always 1 but it is different from the
                # top-level buildnumber.
                if k == 'build_number':
                    continue

                if k == 'request_ids':
                    p['request_ids'] = props[k]
                    continue

                if k == 'request_times':
                    # TODO
                    continue

                if k == 'testresults':
                    # TODO
                    continue

                if k == 'leakStats':
                    # TODO
                    continue

                if k == 'locales':
                    # TODO
                    #p['locales'] = props[k]
                    continue

                # No clue what this is for. It looks like a bug.
                if k == 'make.py[0]:':
                    continue

                if k.endswith('_failure'):
                    continue

                if k not in self.BUILD_PROPERTIES:
                    print(k)
                    continue

                value = props[k]

                # None is null, which is the default (empty) column value.
                if value is None:
                    continue

                target = self.BUILD_PROPERTIES[k]
                target_type = unicode

                if isinstance(target, tuple):
                    target, target_type = target

                if target_type == unicode and not value:
                    continue

                if type(value) != target_type:
                    value = target_type(value)

                p[target] = value

            self._connection.builds.insert_build(bid, 1, p)

        return len(o)

    def parse_logs(self, build_ids):
        """Parse the logs for the specified build IDs into storage."""
        # TODO hook up parallel processing.

        with self._connection.cursor() as c:
            self._parse_logs(build_ids, c)

    def _parse_logs(self, build_ids, c):
        OUR_VERSION = 1

        q_add_build_step = c.prepare_query(b'''
            INSERT INTO build_steps (build_id, i, name, state, results, start,
            end, duration) VALUES (:build_id, :i, :name, :state, :results,
            :start, :end, :duration_float)''')

        q_step_counters = c.prepare_query(b'''
            BEGIN COUNTER BATCH
            UPDATE build_step_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE name = :name;

            UPDATE build_step_category_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE category = :category;

            UPDATE build_step_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE name = :name AND day=:day_utc AND is_utc=true;

            UPDATE build_step_daily_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE name = :name AND day = :day_mv AND is_utc = false;

            UPDATE build_step_daily_category_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE name = :name AND category = :category AND day = :day_utc
                    AND is_utc = true;

            UPDATE build_step_daily_category_counters SET
                number = number + 1,
                duration = duration + :duration
                WHERE name = :name AND category = :category AND day = :day_mv
                    AND is_utc=false;

            APPLY BATCH
        ''')

        epoch = datetime.datetime.utcfromtimestamp(0).replace(tzinfo=pytz.utc)

        for build_id in build_ids:
            info = self._connection.builds.get_build(build_id)
            if not info:
                continue

            existing_version = info.get('log_parse_version')
            if existing_version and existing_version >= OUR_VERSION:
                continue

            log = self._connection.get_file(info['log_url'])
            if not log:
                continue

            parsed = parse_build_log(log)

            for i, step in enumerate(parsed.steps):
                mv_start = step.start
                utc_start = step.start.astimezone(pytz.utc)
                utc_end = step.end.astimezone(pytz.utc)

                start = (utc_start - epoch).total_seconds()
                end = (utc_end - epoch).total_seconds()
                duration = end - start

                params = dict(
                    build_id=build_id,
                    i=i,
                    name=step.name,
                    state=step.state,
                    results=step.results,
                    start=start,
                    end=end,
                    duration_float=duration,
                    duration=int(duration),
                    category=info['builder_category'],
                    day_utc=utc_start.date().isoformat(),
                    day_mv=mv_start.date().isoformat(),
                )

                c.execute_prepared(q_add_build_step, params)
                c.execute_prepared(q_step_counters, params)

            self._connection.builds.update_log_parse_version(build_id,
                OUR_VERSION)

            yield 'Parsed build %s into %d steps.' % (build_id,
                len(parsed.steps))

