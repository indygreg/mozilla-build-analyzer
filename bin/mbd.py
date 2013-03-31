# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from __future__ import unicode_literals

import argparse
import calendar
import sys
import time

from mozbuilddata.buildbotdata import DataLoader as BuildbotDataLoader
from mozbuilddata.cassandra import Connection


def load_job_data(connection=None, day=None):
    t = time.gmtime()
    if day:
        t = time.strptime(day, '%Y-%m-%d')

    t = calendar.timegm(t)

    loader = BuildbotDataLoader(connection.pool)
    result = loader.load_builds_from_day(t)

    print('Loaded %d slaves' % result['slave_count'])
    print('Loaded %d masters' % result['master_count'])
    print('Loaded %d builders' % result['builders_count'])
    print('Loaded %d jobs' % result['build_count'])


def load_raw_logs(connection=None):
    loader = BuildbotDataLoader(connection.pool)
    for msg in loader.load_missing_logs():
        print(msg)


def main():
    parser = argparse.ArgumentParser(
        description='Do stuff with Mozilla build data.')

    parser.add_argument('--host', default=['localhost'], nargs='*',
        help='Cassandra host to connect to.')

    parser.add_argument('--keyspace', default='mozbuilddata',
        help='Cassandra keyspace to use for storage.')

    subparsers = parser.add_subparsers()

    ljd = subparsers.add_parser('load-job-data',
        help='Load information about buildbot jobs into storage.')
    ljd.add_argument('--day',
        help='Day to load. Specified as a YYYY-MM-DD value.')
    ljd.set_defaults(func=load_job_data)

    ll = subparsers.add_parser('load-raw-logs',
        help='Load raw job logs into storage.')
    ll.set_defaults(func=load_raw_logs)

    args = parser.parse_args()

    connection = Connection()
    connection.connect(args.keyspace, servers=args.host)

    global_arguments = ['host', 'keyspace', 'func']
    relevant = {k: getattr(args, k) for k in vars(args) if k not in
        global_arguments}

    relevant['connection'] = connection

    args.func(**relevant)

if __name__ == '__main__':
    main()
