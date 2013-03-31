# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# This file contains code for parsing raw job logs.

from __future__ import unicode_literals

import datetime
import re

from collections import (
    deque,
    namedtuple,
)


RE_HEADER_METADATA = re.compile('^(?P<key>[a-z]+): (?P<value>.*)$')
STARTED = b'========= Started'
FINISHED = b'========= Finished'

ELAPSED = r'''
    (?:(?P<elapsed_hours>\d+)\shrs,\s)?
    (?:(?P<elapsed_minutes>\d+)\smins,\s)?
    (?P<elasped_seconds>\d+)\ssecs
    '''

STEP_COMMON = r'''
    (?P<name>.*)\s\(
    results:\s(?P<results>\d+),\s
    elapsed:\s''' + ELAPSED + '''\)\s
    \(at\s(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})\s
    (?P<hour>\d{2}):(?P<minute>\d{2}):(?P<second>\d{2})\.(?P<subsecond>\d+)\)\s
    =========$'''

RE_STARTED = re.compile('^=========\sStarted\s' + STEP_COMMON, re.VERBOSE)
RE_FINISHED = re.compile('^=========\sFinished\s' + STEP_COMMON, re.VERBOSE)


Step = namedtuple('Step', ('name', 'results', 'elapsed', 'start', 'end', 'lines'))


class ParsedLog(object):
    def __init__(self):
        self.metadata = {}
        self.steps = []

        self._in_step = False


def parse_job_log(log):
    lines = deque(log.splitlines())

    parsed = ParsedLog()

    # Job logs start with a series of key: value metadata.
    while True:
        try:
            line = lines.popleft()
        except IndexError:
            return parsed

        if line == '':
            break

        match = RE_HEADER_METADATA.match(line)
        assert match
        key, value = match.groups()
        parsed.metadata[key] = value

    if len(lines):
        assert lines[0].startswith(STARTED)

    # The bulk of the log should be a bunch of sections identifying the steps
    # this job performed.

    current_step = None
    step_lines = []

    def match_to_datetime(m):
        return datetime.datetime(
            int(m['year']),
            int(m['month']),
            int(m['day']),
            int(m['hour']),
            int(m['minute']),
            int(m['second']),
            int(m['subsecond'])
        )

    while True:
        try:
            line = lines.popleft()
        except IndexError:
            return parsed

        if line.startswith(STARTED):
            match = RE_STARTED.match(line)
            assert match
            assert not current_step

            current_step = match.groupdict()
            continue

        if line.startswith(FINISHED):
            assert current_step
            match = RE_FINISHED.match(line)
            assert match

            g = match.groupdict()

            # Names should be the same.
            assert current_step['name'] == g['name']

            # Do we need to calculate elapsed or can wall time diff suffice?
            elapsed_seconds = 0
            elapsed_seconds += 3600 * int(current_step.get('elapsed_hours') or 0)
            elapsed_seconds += 60 * int(current_step.get('elapsed_minutes') or 0)
            elapsed_seconds += int(current_step.get('elapsed_seconds') or 0)

            start = match_to_datetime(current_step)
            end = match_to_datetime(g)

            parsed.steps.append(Step(g['name'], g['results'], elapsed_seconds,
                start, end, step_lines))

            step_lines = []
            current_step = None
            continue

        if current_step:
            step_lines.append(line)
            continue

        assert not line

    return parsed
