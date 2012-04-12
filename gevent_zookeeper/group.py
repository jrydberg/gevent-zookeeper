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

import os.path

from .monitor import MonitorListener


class Notify(MonitorListener):

    def __init__(self, group):
        self.group = group

    def commit(self):
        self.group.notify()


class PartitionGroup(object):
    """A group that is partitioned to handle a set of resources or
    tasks.
    """

    def __init__(self, framework, base_path, name, notify):
        self.framework = framework
        self.base_path = base_path
        self.notify = notify
        self.name = name
        self.owned = []
        self.members = {}

    def partition(self, tokens, member):
        """Helper method for spreading C{tokens}."""
        assert member in self.members
        v = sorted(tokens)
        s = sorted(self.members.keys())
        i = s.index(member)
        n = len(v) / len(s)
        e = len(v) % len(s)
        start = n * i + min(i, e)
        stop = start + n + (0 if (i + 1) > e else 1)
        t = v[start:stop]
        return t

    def notify(self):
        """Notify group partitioner."""
        self._notify(self.members.keys(), self.name)

    def claim(self, token):
        """Claim a resource from the group."""

    def give_up(self, token):
        """Give back a resource to the group."""

    def start(self):
        """Start the group."""
        self.zpath = self.framework.create().parents_if_needed().as_ephemeral(
            ).for_path(os.path.join(self.base_path, 'members', self.name))
        self.monitor = self.framework.monitor().children().store_into(
            self.members, str).using(Notify(self)).for_path(os.path.join(
                self.base_path, 'members'))

    def close(self):
        """Stop the group."""
        self.monitor.close()
        self.framework.delete().for_path(self.zpath)
