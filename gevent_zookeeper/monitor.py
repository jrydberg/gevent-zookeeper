# Copyright 2012 Johan Rydberg
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os.path

from gevent.event import AsyncResult
from gevent.queue import Queue
import gevent


class MonitorListener:
    """Abstract base class for monitor listeners."""

    def created(self, path, data):
        pass

    def modified(self, path, data):
        pass

    def deleted(self, path):
        pass


class DataMonitor(object):
    """Functionality for monitoring a single node about changes."""

    def __init__(self, client, path, listener):
        self.client = client
        self.listener = listener
        self.greenlet = None
        self.path = path
        self.queue = Queue()

    def _monitor(self):
        """."""
        def watcher(event):
            self.queue.put(event)

        first = False
        while True:
            try:
                data, stat = self.client.get(self.path, watcher)
            except zookeeper.NoNodeException:
                pass
            print "GOT EVENT", self.queue.get()


    def start(self):
        """."""
        self.greenlet = gevent.spawn(self._monitor)

    def close(self):
        """Stop the monitor."""


class ChildrenMonitor(object):
    """Simple monitor that monitors the children of a node and their
    content.
    """
    _STOP_REQUEST = object()

    def __init__(self, client, path, into, factory, listener):
        self.client = client
        self.path = path
        self.into = into if into is not None else {}
        self.factory = factory if factory is not None else str
        self.listener = listener
        self.started = AsyncResult()
        self.queue = Queue()
        self.stats = {}

    def _monitor(self):
        """Run the monitoring loop."""
        def watcher(event):
            self.queue.put(event)

        while True:
            try:
                children = self.client.get_children(self.path, watcher)
            except Exception, err:
                if not self.started.ready():
                    self.started.set_exception(err)
                    break
            else:
                if not self.started.ready():
                    self.started.set(None)

            for child in children:
                if not child in self.stats:
                    data, stat = self.client.get(os.path.join(self.path, child))
                    self.into[child] = self.factory(data)
                    if self.listener:
                        self.listener.created(child, self.into[child])
                    self.stats[child] = stat
                else:
                    data, stat = self.client.get(os.path.join(self.path, child))
                    if stat['version'] != self.stats[child]['version']:
                        self.into[child] = self.factory(data)
                        self.listener.modified(child, self.into[child])
                    self.stats[child] = stat
            for child in self.into.keys():
                if child not in children:
                    del self.into[child]
                    self.listener.deleted(child)

            event = self.queue.get()
            if event is self._STOP_REQUEST:
                break

    def start(self):
        """Start monitoring."""
        self.greenlet = gevent.spawn(self._monitor)
        return self.started.get()

    def close(self):
        """Stop the monitor."""
        self.queue.put(self._STOP_REQUEST)
        self.greenlet.join()
