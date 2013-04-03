# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from threading import (
    Event,
    Thread,
)
from Queue import (
    Empty,
    Queue,
)

import socket
import time

import urllib3


def thread_worker(shutdown, pool, request_queue):
    while not shutdown.is_set():
        try:
            url, callback, callback_args = request_queue.get(True, 1)
        except Empty:
            continue

        try:
            r = pool.request('GET', url)
            if callback:
                args = callback_args or []
                callback(r, *callback_args)
        except socket.gaierror as e:
            print('Exception fetching %s' % url)
            print(e)
        finally:
            request_queue.task_done()


class ParallelHttpFetcher(object):
    """Perform HTTP requests in parallel."""

    def __init__(self, connections=4):
        self.pool = urllib3.PoolManager(maxsize=connections)
        self.queue = Queue()
        self.shutdown = Event()
        self.threads = []

        for i in range(connections * 2):
            t = Thread(target=thread_worker,
                args=(self.shutdown, self.pool, self.queue))
            t.daemon = True
            t.start()
            self.threads.append(t)

    def add_url(self, url, callback=None, callback_args=None):
        self.queue.put((url, callback, callback_args))

    def wait(self):
        try:
            while True:
                if self.queue.empty():
                    self.shutdown.set()
                    break

                time.sleep(1)

            self.queue.join()
        except KeyboardInterrupt:
            self.shutdown.set()

