# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import print_function, unicode_literals

import calendar
import datetime
import numpy
import operator
import time

from mach.decorators import (
    CommandArgument,
    CommandProvider,
    Command,
)

from mozbuilddata.buildbotdata import (
    DataLoader as BuildbotDataLoader,
)

from mozbuilddata.cassandra import connect
from mozbuilddata.logparser.jobparser import parse_build_log


@CommandProvider
class Commands(object):
    def __init__(self, context):
        self.c = connect(context.host, context.port, context.keyspace)

    @Command('download-build-activity', category='downloading',
        description='Download information about build activity.')
    def download_build_activity(self):
        loader = BuildbotDataLoader(self.c)
        for msg in loader.synchronize_build_files():
            print(msg)

    @Command('download-build-logs', category='downloading',
        description='Download output from individual builds.')
    @CommandArgument('--builder-pattern',
        help='Only load logs for builders matching this pattern. The pattern '
            'is a UNIX shell glob style pattern match, performed without '
            'regard to character case.')
    @CommandArgument('--category',
        help='Only consider builds in this category.')
    @CommandArgument('--after',
        help='Only load logs from builds after this date (YYYY-MM-DD).')
    def download_build_logs(self, **kwargs):
        loader = BuildbotDataLoader(self.c)
        for msg in loader.load_missing_logs(**kwargs):
            print(msg)

    @Command('load-builds', category='loading',
        description='Load build metadata from JSON files.')
    @CommandArgument('after',
        help='Load data from before this YYYY-MM-DD date.')
    @CommandArgument('before',
        help='Load data from after this YYYY-MM-DD date.')
    def load_builds(self, before=None, after=None):
        before = datetime.datetime.strptime(before, '%Y-%m-%d')
        after = datetime.datetime.strptime(after, '%Y-%m-%d')
        day = datetime.timedelta(1)

        loader = BuildbotDataLoader(self.c)

        current = before - day
        while current >= after:
            print('Loading data for %s' % current.date().isoformat())
            for msg in loader.load_builds_from_day(current):
                print(msg)

            current -= day

    @Command('load-logs', category='loading',
        description='Parse logs and load derived data into storage.')
    @CommandArgument('build_id', nargs='*', type=int,
        help='The IDs of builds whose logs to parse and load.')
    def load_logs(self, build_id):
        loader = BuildbotDataLoader(self.c)
        for msg in loader.parse_logs(build_id):
            print(msg)

    @Command('truncate-build-data', category='cassandra',
        description='Truncate loaded build metadata. This erases all data '
            'directly derived from build events. This does not erase data '
            'from parsed logs. After performing this, you will need to '
            're-load all build metadata.')
    def truncate_build_data(self):
        self.c.truncate_build_metadata()

    @Command('drop-build-tables', category='cassandra',
        description='Drop all tables holding build metadata. This is '
            'similar to truncate-build-data in that it deletes lots of data. '
            'The main difference is this drops Cassandra tables instead of '
            'merely truncating.')
    def drop_build_tables(self):
        self.c.drop_build_tables()

    @Command('truncate-log-data', category='cassandra',
        description='Truncate data derived from parsing logs.')
    def truncate_log_data(self):
        self.c.truncate_log_metadata()

    @Command('drop-log-tables', category='cassandra',
        description='Drop all tables holding data derived from parsed logs.')
    def drop_log_tables(self):
        self.c.drop_log_tables()

    @Command('builder-names', category='builders',
        description='Print all builder names.')
    def builder_names(self):
        builders = self.c.builders()

        for name in sorted(set(t[1] for t in builders)):
            print(name)

    @Command('builder-categories', category='builders',
        description='Print all builder categories.')
    def builder_categories(self):
        builders = self.c.builders()

        for category in sorted(set(t[2] for t in builders)):
            print(category)

    @Command('builders-in-category', category='builders',
        description='Print all builders in a specific category.')
    @CommandArgument('category',
        help='The category whose builders to print.')
    def builders_in_category(self, category):
        names = self.c.builder_names_in_category(category)

        for builder in sorted(set(names)):
            print(builder)

    @Command('build-counts-by-builder', category='builders',
        description='Print build counts by builder.')
    def build_counts_by_builder(self):
        m = self.c.builder_name_map()
        d = {}

        for bid, count in self.c.builder_counts():
            name = m.get(bid, None)
            if not name:
                continue

            d[name] = count

        for name in sorted(d):
            print('%d\t%s' % (d[name], name))

    @Command('builder-times-statistics', category='builders',
        description='Print statistics for builder times.')
    @CommandArgument('--statistic', choices=['count', 'histogram',
        'median', 'mean', 'stddev', 'sum', 'variance'],
        help='Which statistic to print.')
    @CommandArgument('--category',
        help='Limit output to builders in this category.')
    def builder_times_statistics(self, statistic=None, category=None):
        builders = self.c.builders()

        if category:
            builders = [t for t in builders if t[2] == category]

        for builder in sorted(set(t[1] for t in builders)):
            times = []
            for build_id, duration in \
                self.c.build_durations_with_builder_name(builder):

                times.append(duration)

            if not len(times):
                continue

            times = numpy.array(times)
            reverse = False

            if statistic == 'count':
                value = len(times)
            elif statistic == 'histogram':
                counts, bins = numpy.histogram(times, bins=15)
                values = []
                for i, count in enumerate(counts):
                    values.append('%d,%d' % (counts[i], bins[i]))
                value = '\t'.join(values)
                reverse = True
            elif statistic == 'mean':
                value = numpy.mean(times)
            elif statistic == 'median':
                value = numpy.median(times)
            elif statistic == 'stddev':
                value = numpy.std(times)
            elif statistic == 'sum':
                value = numpy.sum(times)
            elif statistic == 'variance':
                value = numpy.var(times)
            else:
                raise Exception('Unknown statistic: %s' % statistic)

            if reverse:
                print('%s\t%s' % (builder, value))
            else:
                print('%s\t%s' % (value, builder))

    @Command('slave-names', category='slaves',
        description='Print the names of all slaves.')
    def slave_names(self):
        names = set(t[1] for t in self.c.slaves())
        for slave in sorted(names):
            print(slave)

    @Command('slave-build-ids', category='slaves',
        description='Print the build IDs that executed on a specific slave.')
    @CommandArgument('name',
        help='The name of the slave whose builds to show.')
    def slave_build_ids(self, name):
        ids = self.c.build_ids_on_slave(name)
        for build_id in ids:
            print(build_id)

    @Command('slave-efficiencies', category='slaves',
        description='Print efficiency information about slaves.')
    def slave_efficiencies(self):
        slaves = sorted((t[1], t[0]) for t in self.c.slaves())

        print('slave\ttotal time (s)\tused time (s)\tunused time (s)\t'
            'efficiency')

        for slave_name, slave_id in slaves:
            times = []

            build_ids = self.c.build_ids_on_slave(slave_name)
            for build_id in sorted(build_ids):
                info = self.c.builds.get_build(build_id)
                if not info:
                    continue

                times.append((info['start_time'], info['end_time']))

            if not times:
                continue

            times = sorted(times, key=operator.itemgetter(0))

            earliest = times[0][0]
            latest = times[-1][1]
            total = latest - earliest
            used = 0
            unused = 0

            last = earliest
            for start, end in times:
                unused = start - last
                used += end - start
                last = end

            efficiency = float(used) / total * 100

            print('%s\t%d\t%d\t%d\t%.1f' % (slave_name, total, used, unused,
                efficiency))

    @Command('builds-in-category', category='builds',
        description='List all builds belonging to a specific category.')
    @CommandArgument('category',
        help='Category to query.')
    def builds_in_category(self, category):
        build_ids = sorted(self.c.build_ids_in_category(category))

        for bid in build_ids:
            print(bid)

    @Command('builds-for-builder', category='builds',
        description='List all builds for a specific builder')
    @CommandArgument('builder',
        help='Name of the builder whose builds to show.')
    def builds_for_builder(self, builder):
        build_ids = self.c.build_ids_with_builder_name(builder)

        for bid in sorted(build_ids):
            print(bid)

    @Command('build-times', category='builds',
        description='Print times it took builds to run.')
    @CommandArgument('--category',
        help='Only include builds in this category.')
    @CommandArgument('builder', nargs='*',
        help='Name of builder to query. Overrides other arguments.')
    def build_times(self, category=None, builder=None):
        if category and builder:
            print('Cannot specify both --category and explicit builders.')
            return 1

        print_builder = False
        build_ids = set()

        if category:
            print_builder = True
            build_ids = set(self.c.build_ids_in_category(category))
        else:
            for builder_name in builder:
                build_ids |= \
                    set(self.c.build_ids_with_builder_name(builder_name))

        for build_id in sorted(build_ids):
            info = self.c.builds.get_build(build_id)
            if not info:
                continue

            print(build_id, '\t', end='')

            elapsed_seconds = info['end_time'] - info['start_time']
            elapsed = datetime.timedelta(seconds=elapsed_seconds)
            print(elapsed_seconds, '\t', elapsed, '\t', end='')

            if print_builder:
                print(info['builder_name'], '\t', end='')

            print('')

    @Command('log-cat', category='logs',
        description='Print a raw build log.')
    @CommandArgument('build_id', nargs='+',
        help='The ID of the build whose log to print.')
    def log_cat(self, build_id):
        for bid in build_id:
            print(self.c.build_log(bid))

    @Command('log-steps', category='logs',
        description='Print information on steps in logs.')
    @CommandArgument('build_id', nargs='+', type=int,
        help='ID of the build to analyze.')
    def build_steps(self, build_id):
        for bid in build_id:
            log = self.c.build_log(bid)
            if not log:
                continue

            parsed = parse_build_log(log)
            for step in parsed.steps:
                print('%s %s %s %s' % (
                    step.start.isoformat(), step.end.isoformat(),
                    step.end - step.start,
                    step.name))
