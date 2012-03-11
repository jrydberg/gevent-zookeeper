# Copyright 2012 Johan Rydberg.
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

import os
import os.path
import zookeeper

from gevent_zookeeper.client import ZookeeperClient
from gevent_zookeeper.monitor import DataMonitor, ChildrenMonitor


__all__ = ['ZookeeperFramework']


# Unside ACL.
ZOO_OPEN_ACL_UNSAFE = {
    "perms": zookeeper.PERM_ALL,
    "scheme": "world",
    "id": "anyone"
}


class GetBuilder(object):
    """Builder for C{get} of C{ZookeeperFramework}."""

    def __init__(self, client):
        self.client = client
        self.watcher = None

    def using_watcher(self, watcher):
        self.watcher = watcher
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        result = self.client.client.get(path, watcher=self.watcher)
        return result[0]


class GetChildrenBuilder(object):
    """Builder for C{get_children} of C{ZookeeperFramework}."""

    def __init__(self, client):
        self.client = client
        self.watcher = None

    def using_watcher(self, watcher):
        self.watcher = watcher
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        return self.client.client.get_children(path, watcher=self.watcher)


class MonitorBuilder(object):
    """Builder for C{monitor} of C{ZookeeperFramework}."""

    def __init__(self, client):
        self.client = client
        self.listener = None
        self.factory = None
        self.into = None
        self.item_store = {}
        self.item_factory = None

    def content(self):
        self.monitor_factory = DataMonitor
        return self

    def children(self):
        self.monitor_factory = ChildrenMonitor
        return self

    def using(self, listener):
        self.listener = listener
        return self

    def store_into(self, target, factory):
        self.item_store = target
        self.item_factory = factory
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        monitor = self.monitor_factory(self.client.client, path,
            self.item_store, self.item_factory, self.listener)
        monitor.start()
        return monitor


class CreateBuilder(object):
    """Builder for C{create} of C{ZookeeperFramework}."""

    def __init__(self, client):
        self.client = client
        self.create_parents = False
        self.data = ''
        self.flags = 0
        self.acl = [ZOO_OPEN_ACL_UNSAFE]

    def parents_if_needed(self):
        self.create_parents = True
        return self

    def as_ephemeral(self):
        self.flags = self.flags | zookeeper.EPHEMERAL
        return self

    def in_sequence(self):
        self.flags = self.flags | zookeeper.SEQUENCE
        return self

    def with_flags(self, flags):
        self.flags = flags
        return self

    def with_data(self, data):
        self.data = data
        return self

    def for_path(self, path):
        """Execute the path."""
        path = self.client._adjust_path(path)
        if self.create_parents:
            self.client.create_parents(path, acl=self.acl)
        return self.client.client.create(path, self.data,
                                         self.acl, self.flags)


class ZookeeperFramework(object):
    """High-level framework for talking to a zookeeper cluster.

    This framework provides a simple API to working with ZooKeeper.
    The API is design around the idea of "chained function calls".

    @param hosts: A comma-separated list of hosts to connect to.
    @type hosts: C{str}

    @param timeout: The default timeout for (some) operations.
    @type timeout: a number

    @param chroot: An optional change root jail of the framework.

    """

    def __init__(self, hosts, timeout, chroot=None):
        self.client = ZookeeperClient(hosts, timeout)
        self.chroot = chroot

    def _adjust_path(self, path):
        """Adjust C{path} so that it will possibly be put inside the
        change root.
        """
        return path if not self.chroot else os.path.join(self.chroot, path)

    def connect(self, timeout=None):
        """Connect to the zookeeper cluster.

        @param timeout: An optional timeout.  If not specified, the
            timeout given to C{ZookeeperFramework} will be used.
        """
        return self.client.connect(timeout)

    def create(self):
        """Create a node.

        Returns a builder that has to be terminated with a C{for_path}
        call.

        Use the C{with_data} method of the builder to set the data new
        node:

             >>> framework.create().with_data('x').for_path('/node')

        If the node you wanna create is located under some other nodes
        that you're not sure exist, you can use the C{parents_if_needed}
        method:

             >>> framework.create().parents_if_needed().for_path('/node')

        Flags of the new node can be set using C{with_flags} or directly
        with C{as_ephemeral} or C{in_sequence}:

             >>> framework.create().as_ephemeral().for_path('/node')

        Rethrn C{for_path} call will return the created node path.
        """
        return CreateBuilder(self)

    def get(self):
        """Get the data content of node.

        Returns a builder that has to be terminated with a C{for_path}
        call.

        The builder has a C{with_watcher} method that can be used to
        associate a watcher will the node.  The watcher will get called
        the next time there's a change to the node.  Note that watchers
        are triggered only once:

             >>> framework.get().with_watcher(watcher).for_path('/a')

        C{watcher} is a one-argument callable that will be invoked with
        a structure describing the event.
        """
        return GetBuilder(self)

    def get_children(self):
        """Return a list of all children of a node.

        Returns a builder that has to be terminated with a C{for_path}
        call.

        The builder has a C{with_watcher} method that can be used to
        associate a watcher will the node.  The watcher will get
        called the next time a child is created or deleted.  Note that
        watchers are triggered only once:

             >>> framework.get_children().with_watcher(watcher).for_path('/a')

        C{watcher} is a one-argument callable that will be invoked with
        a structure describing the event.
        """
        return GetChildrenBuilder(self)

    def create_parents(self, path, value='', acl=None, flags=0):
        components = path.split(os.sep)
        for i in range(2, len(components)):
            path = '/' + os.path.join(*components[1:i])
            try:
                self.client.create(path, value, acl, flags)
            except zookeeper.NodeExistsException:
                pass

    def monitor(self):
        """Start monitoring a node or its children.

        Returns a builder that has to be terminated with C{for_path}.
        The builder can be used to setup monitoring for content of
        a single node or for all children of a node.

        To monitor all the children of a node, where C{listener} is a
        C{MonitorListener} instance:

            >>> monitor = framework.monitor().children().using(
                  listener).for_path('/test')

        If you wanna build up a local cache of the state, the
        "store_into" method can be of interest:

            >>> monitor = framework.monitor().children().store_into(
                  collection, factory).for_path('/test')

        Here C{collection} is a C{dict}-like object and C{factory} is
        a callabe that will be used to construct the items to put into
        the collection.  The factory will be called with the content
        of a node (as a string) as the sole argument.

        The builder's C{for_path} method return a Monitor instance
        that has C{close} method that can be invoked to stop the
        monitoring:

            >>> monitor.close()

        """
        return MonitorBuilder(self)
