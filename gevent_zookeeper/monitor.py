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
import zookeeper
import logging
import random


from gevent.event import AsyncResult
from gevent.queue import Queue
import gevent


class DataMonitor(object):
    _STOP_REQUEST = object()

    def __init__(self, client, path, callback, args, kwargs):
        self.client = client
        self.path = path
        self.callback = callback
        self.args = args
        self.kwargs = kwargs
        self.started = AsyncResult()
        self.queue = Queue()
        self._delay = 1.343
        self.max_delay = 180

    def _monitor(self):
        """Run the monitoring loop."""
        def watcher(event):
            self.queue.put(event)

        while True:
            try:
                data, stat = self.client.get(self.path, watcher)
            except zookeeper.NoNodeException:
                if not self.started.ready():
                    self.started.set(None)
                gevent.sleep(1)
                continue
            except (zookeeper.ConnectionLossException), err:
                if not self.started.ready():
                    self.started.set_exception(err)
                    break
                logging.error("got %r while monitoring %s", str(err),
                              self.path)
                gevent.sleep(self._delay)
                self._delay += self._delay * random.random()
                self._delay = min(self._delay, self.max_delay)
                continue
            except Exception, err:
                if not self.started.ready():
                    self.started.set_exception(err)
                    break
                raise
                
            self.callback(data, *self.args, **self.kwargs)

            if not self.started.ready():
                self.started.set(None)

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


class MonitorListener:
    """Abstract base class for monitor listeners."""

    def created(self, path, data):
        pass

    def modified(self, path, data):
        pass

    def deleted(self, path):
        pass

    def commit(self):
        pass


class CallbackMonitorListener(MonitorListener):

    def __init__(self, callback, args, kwargs):
        self.callback = callback
        self.args = args
        self.kwargs = kwargs

    def commit(self):
        self.callback(*self.args, **self.kwargs)


class ChildrenMonitor(object):
    """Simple monitor that monitors the children of a node and their
    content.
    """
    _STOP_REQUEST = object()

    def __init__(self, client, path, into, factory, args, listener):
        self.client = client
        self.path = path
        self.into = into if into is not None else {}
        self.factory = factory if factory is not None else str
        self.args = args
        self.listener = listener or MonitorListener()
        self.started = AsyncResult()
        self.queue = Queue()
        self.stats = {}
        self._delay = 1.343
        self.max_delay = 180

    def _monitor(self):
        """Run the monitoring loop."""
        def watcher(event):
            self.queue.put(event)

        while True:
            try:
                children = self.client.get_children(self.path, watcher)
            except zookeeper.NoNodeException:
                if not self.started.ready():
                    self.started.set(None)
                gevent.sleep(1)
                continue
            except (zookeeper.ConnectionLossException), err:
                if not self.started.ready():
                    self.started.set_exception(err)
                    break
                logging.error("got %r while monitoring %s", str(err),
                              self.path)
                gevent.sleep(self._delay)
                self._delay += self._delay * random.random()
                self._delay = min(self._delay, self.max_delay)
                continue
            except Exception, err:
                if not self.started.ready():
                    self.started.set_exception(err)
                    break
                raise

            for child in children:
                if not child in self.stats:
                    try:
                        data, stat = self.client.get(os.path.join(self.path, child))
                    except zookeeper.NoNodeException:
                        print "race condition while getting", os.path.join(
                            self.path, child)
                    else:
                        self.into[child] = self.factory(data, *self.args)
                        self.listener.created(child, self.into[child])
                        self.stats[child] = stat
                else:
                    try:
                        data, stat = self.client.get(os.path.join(self.path, child))
                    except zookeeper.NoNodeException:
                        print "race condition while getting", os.path.join(
                            self.path, child)
                        # should we remove it here?
                    else:
                        if stat['version'] != self.stats[child]['version']:
                            self.into[child] = self.factory(data, *self.args)
                            self.listener.modified(child, self.into[child])
                        self.stats[child] = stat
            for child in self.into.keys():
                if child not in children:
                    del self.into[child]
                    del self.stats[child]
                    self.listener.deleted(child)

            if not self.started.ready():
                self.started.set(None)

            self.listener.commit()

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
