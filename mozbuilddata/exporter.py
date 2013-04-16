# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import errno
import json
import os


class JSONExporter(object):
    def __init__(self, conn, path):
        self.c = conn
        self.root = path

    def export(self):
        self._mkdir('.')

        for f in ['totals', 'builder_counters']:
            func = getattr(self, '_export_%s' % f)

            for msg in func():
                yield msg

    def _mkdir(self, p):
        p = os.path.join(self.root, p)

        try:
            os.makedirs(p)
        except OSError as e:
            if e.errno != errno.EEXIST:
                raise

    def _active_dates(self):
        today = datetime.date.today()

        for i in range(30, 0, -1):
            yield today - datetime.timedelta(i)


    def _write_obj(self, path, obj):
        path = os.path.join(self.root, path)

        with open(path, 'wb') as fh:
            json.dump(obj, fh)

    def _export_totals(self):
        yield 'Writing totals.json'
        o = {}
        #o['file_counts'] = len(list(self.c.filenames()))

        self._write_obj('totals.json', o)

    def _export_builder_counters(self):
        yield 'Writing builder counts files.'
        self._mkdir('builder/job_counts/by-day')
        self._mkdir('builder/job_durations/by-day')

        counts = dict(self.c.builder_counts())
        self._write_obj('builder/job_counts/all.json', counts)

        for date in self._active_dates():
            df = date.isoformat()
            counts = dict(self.c.builder_counts_in_day(df))

            if not counts:
                continue

            self._write_obj('builder/job_counts/by-day/%s.json' % df, counts)

        yield 'Writing builder duration files.'
        durations = dict(self.c.builder_durations())
        self._write_obj('builder/job_durations/all.json', durations)

        for date in self._active_dates():
            df = date.isoformat()
            durations = dict(self.c.builder_durations_in_day(df))

            if not durations:
                continue

            self._write_obj('builder/job_durations/by-day/%s.json' % df,
                durations)

        yield 'Writing per-category builder files.'
        for cat in sorted(self.c.builder_categories()):
            p = 'builder/by-category/%s' % cat
            self._mkdir(p)

            counts = dict(self.c.builder_counts_in_category(cat))
            total = sum(counts.values())

            self._write_obj('%s/job-counts.json' % p, counts)

