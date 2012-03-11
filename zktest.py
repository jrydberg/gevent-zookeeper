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

import sys

from gevent.monkey import patch_all; patch_all()
import gevent

from gevent_zookeeper import ZookeeperFramework, MonitorListener


class Listener(MonitorListener):
    """Example of a monitor listener."""

    def created(self, path, item):
        print "created", path, repr(item)

    def modified(self, path, item):
        print "modified", path, repr(item)

    def deleted(self, path):
        print "deleted", path

store = {}
listener = Listener()

framework = ZookeeperFramework(sys.argv[1], 10)
framework.connect()

framework.monitor().children().using(listener).store_into(
    store, int).for_path('/test')

while True:
    result = framework.create().as_ephemeral().in_sequence().parents_if_needed(
        ).with_data('1111').for_path('/test/aaa')
    gevent.sleep(5)
