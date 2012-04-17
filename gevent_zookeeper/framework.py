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
from gevent_zookeeper.monitor import (DataMonitor, ChildrenMonitor,
                                      CallbackMonitorListener)


__all__ = ['ZookeeperFramework']


# Unside ACL.
ZOO_OPEN_ACL_UNSAFE = {
    "perms": zookeeper.PERM_ALL,
    "scheme": "world",
    "id": "anyone"
}


class DeleteBuilder(object):
    """Builder for C{delete} of C{ZookeeperFramework}."""

    def __init__(self, client):
        self.client = client
        self.ignore_errors = None

    def ignoring_errors(self):
        self.ignore_errors = True
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        try:
            self.client.client.delete(path)
        except zookeeper.NoNodeException:
            if not self.ignore_errors:
                raise


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


class DataMonitorBuilder(object):
    """Builder for monitoring of data."""

    def __init__(self, client):
        self.client = client
        self.callback = None
        self.args = None
        self.kwargs = None

    def using(self, fn, *args, **kwargs):
        self.callback = fn
        self.args = args
        self.kwargs = kwargs
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        monitor = DataMonitor(self.client.client, path,
            self.callback, self.args, self.kwargs)
        monitor.start()
        return monitor


class ChildrenMonitorBuilder(object):
    """Child monitor builder."""

    def __init__(self, client):
        self.client = client
        self.listener = None
        self.factory = None
        self.into = None
        self.item_store = {}
        self.item_factory = None
        self.monitor_factory = ChildrenMonitor

    def using(self, listener):
        self.listener = listener
        return self

    def notifying(self, callback, *args, **kwargs):
        self.listener = CallbackMonitorListener(callback,
            args, kwargs)
        return self

    def store_into(self, target, factory, *args):
        self.item_store = target
        self.item_factory = factory
        self.factory_args = args
        return self

    def for_path(self, path):
        path = self.client._adjust_path(path)
        monitor = self.monitor_factory(self.client.client, path,
            self.item_store, self.item_factory, self.factory_args,
            self.listener)
        monitor.start()
        return monitor


class MonitorBuilder(object):
    """Builder for C{monitor}."""

    def __init__(self, client):
        self.client = client

    def data(self):
        return DataMonitorBuilder(self.client)

    def children(self):
        return ChildrenMonitorBuilder(self.client)


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


class SetBuilder(CreateBuilder):
    """Builder for C{set} of C{ZookeeperFramework}."""

    def __init__(self, client):
        CreateBuilder.__init__(self, client)
        self.create = False

    def create_if_needed(self):
        self.create = True
        return self

    def for_path(self, path):
        """Execute the path."""
        path = self.client._adjust_path(path)
        if self.create_parents:
            self.client.create_parents(path, acl=self.acl)
        try:
            self.client.client.set(path, self.data)
        except zookeeper.NoNodeException:
            if not self.create:
                raise
            self.client.client.create(path, self.data,
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

    def close(self):
        self.client.close()

    def set(self):
        """Set content of a node.

        Returns a builder that has to be terminated with a C{for_path}
        call.

        Use the C{with_data} method of the builder to set the data new
        node:

             >>> framework.set().with_data('x').for_path('/node')

        It is also possible to create the node if it does not exist
        using the C{create_if_needed} method.  The builder accepts the
        same methods as the builder for C{create} when it comes to
        specifying flags.
        """
        return SetBuilder(self)

    def delete(self):
        """Delete a node.

        Returns a builder that has to be terminated with a C{for_path}
        call.

        It is possible to have the framework consume I{no node} errors
        using the C{ignoring_errors} method:

            >>> framework.delete().ignoring_errors().for_path('/xxx')

        """
        return DeleteBuilder(self)

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
