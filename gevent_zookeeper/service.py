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

"""Service registry functionality for working with Netflix Curator."""

import json
import os.path
import zookeeper


class ServiceInstance(object):
    """Representation of a service instance."""

    def __init__(self, name, id, address, port, payload,
                 registration_time, service_type):
        self.name = name
        self.id = id
        self.address = address
        self.port = port
        self.payload = payload
        self.registration_time = registration_time
        self.service_type = service_type

    @classmethod
    def from_data(cls, data):
        """Create a service instance for a piece of raw data."""
        data = json.loads(data)
        return cls(data['name'], data['id'], data['address'], data['port'],
            data['payload'], data['registrationTimeUTC'], data['serviceType'])

    def to_data(self):
        """Return service instance as a JSON object."""
        data = {'name': self.name, 'id': self.id, 'address': self.address,
            'port': self.port, 'payload': self.payload,
            'registrationTimeUTC': self.registration_time,
            'serviceType': self.service_type}
        return json.dumps(data)

    def __repr__(self):
        """Return a string representation of this instance."""
        return ("<ServiceInstance name=%s id=%s address=%s "
                     + "port=%d payload=%s>") % (
            self.name, self.id, self.address, self.port, self.payload)
    __str__ = __repr__


class Registry(object):
    """Base class for instance registries."""

    def __init__(self, framework, base_path, cls):
        self.framework = framework
        self.base_path = base_path
        self.cls = cls
        self.instances = {}

    def start(self):
        self.monitor = self.framework.monitor().children().store_into(
            self.instances, self.cls.from_data).for_path(self.base_path)
        
    def close(self):
        self.monitor.close()

    def register(self, inst):
        """Register an instance."""
        self.framework.create().with_data(inst.to_data()).as_ephemeral(
            ).for_path(os.path.join(self.base_path, inst.id))

    def unregister(self, inst):
        """Unregister an instance."""
        self.framework.delete().for_path(os.path.join(self.base_path,
            inst.id))


class ServiceInstanceRegistry(Registry):
    """Instance registry for a service."""

    def __init_(self, framework, name):
        Registry.__init__(self, framework, os.path.join('/service', name),
                          ServiceInstance)


class Discovery(object):
    """Raw access to a registry."""

    def __init__(self, framework, base_path, cls):
        self.framework = framework
        self.cls = cls
        self.base_path = base_path

    def query_for_names(self):
        """Return names."""
        return self.framework.children().for_path(self.base_path)

    def query_for_instances(self, name):
        """Return instance names."""
        return self.framework.children().for_path(os.path.join(
                self.base_path, name))

    def query_for_instance(self, name, instance_id):
        """Return a specific instance."""
        data = self.framework.get().for_path(os.path.join(
                self.base_path, name, instance_id))
        return self.cls.from_data(data)

    def register_instance(self, instance):
        """Register instance with registry."""
        zpath = os.path.join(self.base_path, instance.name, instance.id)
        return self.framework.create().parents_if_needed().with_data(
            instance.to_data()).as_ephemeral().for_path(zpath)

    def unregister_instance(self, name, instance_id):
        """Unregister instance."""
        zpath = os.path.join(self.base_path, name, instance_id)
        try:
            return self.framework.delete().for_path(zpath)
        except zookeeper.NoNodeException:
            pass
