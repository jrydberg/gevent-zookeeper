# Copyright 2011 David LaBissoniere
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

"""Basic low-level zookeeper client based on the work by labisso [1]

License unknown, assuming something Apache License compatible.

Some code (the ClientEvent class) is from pykeeper [2]

 [1] https://github.com/labisso/zkproto.
 [2] https://github.com/nkvoll/pykeeper
"""

# this client is inspired by the threadpool recipe in the geventutil package:
#
# https://bitbucket.org/denis/gevent-playground/src/tip/geventutil/threadpool.py

import fcntl
import os
import Queue

import gevent
import gevent.event
import zookeeper
from collections import namedtuple

TYPE_NAME_MAPPING = {
    zookeeper.NOTWATCHING_EVENT: "not-watching",
    zookeeper.SESSION_EVENT: "session",
    zookeeper.CREATED_EVENT: "created",
    zookeeper.DELETED_EVENT: "deleted",
    zookeeper.CHANGED_EVENT: "changed",
    zookeeper.CHILD_EVENT: "child"
}

STATE_NAME_MAPPING = {
    zookeeper.ASSOCIATING_STATE: "associating",
    zookeeper.AUTH_FAILED_STATE: "auth-failed",
    zookeeper.CONNECTED_STATE: "connected",
    zookeeper.CONNECTING_STATE: "connecting",
    zookeeper.EXPIRED_SESSION_STATE: "expired",
    # TODO: Find a better name for this?
    999: 'connecting',
    0: 'connecting'
}

class ClientEvent(namedtuple("ClientEvent", 'type, connection_state, path')):
    """A client event is returned when a watch deferred fires. It denotes
    some event on the zookeeper client that the watch was requested on.
    """

    @property
    def type_name(self):
        return TYPE_NAME_MAPPING[self.type]

    @property
    def state_name(self):
        return STATE_NAME_MAPPING[self.connection_state]

    def __repr__(self):
        return  "<ClientEvent %s at %r state: %s>" % (
            self.type_name, self.path, self.state_name)


# this dictionary is a port of err_to_exception() from zkpython zookeeper.c
_ERR_TO_EXCEPTION = {
    zookeeper.SYSTEMERROR: zookeeper.SystemErrorException,
    zookeeper.RUNTIMEINCONSISTENCY: zookeeper.RuntimeInconsistencyException,
    zookeeper.DATAINCONSISTENCY: zookeeper.DataInconsistencyException,
    zookeeper.CONNECTIONLOSS: zookeeper.ConnectionLossException,
    zookeeper.MARSHALLINGERROR: zookeeper.MarshallingErrorException,
    zookeeper.UNIMPLEMENTED: zookeeper.UnimplementedException,
    zookeeper.OPERATIONTIMEOUT: zookeeper.OperationTimeoutException,
    zookeeper.BADARGUMENTS: zookeeper.BadArgumentsException,
    zookeeper.APIERROR: zookeeper.ApiErrorException,
    zookeeper.NONODE: zookeeper.NoNodeException,
    zookeeper.NOAUTH: zookeeper.NoAuthException,
    zookeeper.BADVERSION: zookeeper.BadVersionException,
    zookeeper.NOCHILDRENFOREPHEMERALS: zookeeper.NoChildrenForEphemeralsException,
    zookeeper.NODEEXISTS: zookeeper.NodeExistsException,
    zookeeper.INVALIDACL: zookeeper.InvalidACLException,
    zookeeper.AUTHFAILED: zookeeper.AuthFailedException,
    zookeeper.NOTEMPTY: zookeeper.NotEmptyException,
    zookeeper.SESSIONEXPIRED: zookeeper.SessionExpiredException,
    zookeeper.INVALIDCALLBACK: zookeeper.InvalidCallbackException,
    zookeeper.SESSIONMOVED: zookeeper.SESSIONMOVED,
}

def err_to_exception(error_code, msg=None):
    """Return an exception object for a Zookeeper error code
    """
    try:
        zkmsg = zookeeper.zerror(error_code)
    except Exception:
        zkmsg = ""

    if msg:
        if zkmsg:
            msg = "%s: %s" % (zkmsg, msg)
    else:
        msg = zkmsg

    exc = _ERR_TO_EXCEPTION.get(error_code)
    if exc is None:

        # double check that it isn't an ok resonse
        if error_code == zookeeper.OK:
            return None

        # otherwise generic exception
        exc = Exception
    return exc(msg)


def _pipe():
    """Create a pipe and set the two parts in non-blocking mode."""
    r, w = os.pipe()
    fcntl.fcntl(r, fcntl.F_SETFL, os.O_NONBLOCK)
    fcntl.fcntl(w, fcntl.F_SETFL, os.O_NONBLOCK)
    return r, w


def _pipe_read_callback(event, eventtype, *args):
    try:
        os.read(event.fd, 1)
    except EnvironmentError:
        pass
    while True:
        try:
            async_result, isexc, value, greenlet = event.arg.get(False)
        except Queue.Empty:
            break
        else:
            if isexc:
                async_result.set_exception(value)
                if greenlet is not None:
                    gevent.kill(greenlet)
            else:
                async_result.set(value)


class ZooAsyncResult(gevent.event.AsyncResult):
    """An async result that also send a notifaction over a pipe."""

    def __init__(self, pipe):
        self._pipe = pipe
        gevent.event.AsyncResult.__init__(self)

    def set_exception(self, exception):
        gevent.event.AsyncResult.set_exception(self, exception)
        #os.write(self._pipe, '\0')

    def set(self, value=None):
        gevent.event.AsyncResult.set(self, value)
        #os.write(self._pipe, '\0')


def _watcher_greenlet(async_result, watcher_fun):
    #wait for the result and feed it into the function
    event_type, conn_state, path = async_result.get()
    watcher_fun(ClientEvent(event_type, conn_state, path))


class SessionEventListener:
    """Abstract base class for session event listeners."""

    def session_event(self, event):
        pass


class ZookeeperClient(object):
    """A gevent-friendly wrapper of the Apache Zookeeper zkpython client

    zkpython supports asynchronous operations where a callback function is
    specified, but the callback happens on a different OS thread. This wrapper
    client uses an os.pipe() and a modified subclass of AsyncResult to trigger
    events back to the gevent thread.

    TODO lots to do:
    * handling of ZK client session events
        * long running greenlet triggered by os.pipe() trick that pulls events
          off of a threadsafe queue?
    * cleanup of watcher greenlets?
    * disconnected state handling
    * tests
    * the rest of the operations

    @ivar hosts: The zookeeper hosts to connect to
    @type hosts: A comma-separated lists of hostname:port elements.
    """

    def __init__(self, hosts, timeout):
        self._hosts = hosts
        self._timeout = timeout

        self._pipe_read, self._pipe_write = _pipe()
        self.queue = Queue.Queue()

        self._event = gevent.core.event(
            gevent.core.EV_READ | gevent.core.EV_PERSIST,
            self._pipe_read, _pipe_read_callback, self.queue)
        self._event.arg = self.queue
        self._event.add()

        self._connected = False
        self._connected_async_result = self._new_async_result()

        self._session_event_listeners = []

    def _queue_result(self, async_result, isexc, value=None,
                      greenlet=None):
        self.queue.put((async_result, isexc, value, greenlet))
        os.write(self._pipe_write, '\0')

    def add_session_event_listener(self, listener):
        self_session_event_listeners.append(listener)

    def __del__(self):
        # attempt to clean up the FD from the gevent hub
        if self._event:
            try:
                self._event.cancel()
            except Exception:
                pass

    def _new_async_result(self):
        return ZooAsyncResult(self._pipe_write)

    def _setup_watcher(self, fun):
        if fun is None:
            return None, None

        # create an AsyncResult for this watcher
        async_result = self._new_async_result()

        def callback(handle, *args):
            async_result.set(args)

        greenlet = gevent.spawn(_watcher_greenlet, async_result, fun)

        return callback, greenlet

    def _cleanup_watcher(self, greenlet):
        greenlet.kill()

    @property
    def connected(self):
        """True if connected to the zookeeper cluster."""
        return self._connected

    def _session_watcher(self, handle, type, state, path):
        #print "session watcher", handle, type, state, path

        event = ClientEvent(type, state, path)

        if not self._connected:
            self._queue_result(self._connected_async_result, False)
        self._connected = True

        for listener in self._session_event_listeners:
            listener.session_event(event)

    def connect(self, timeout=None):
        """Connect to the zookeeper cluster.

        @param timeout: Seconds to wait before timing out.  If not
            specified the default timeout for the client will be used.
        """
        timeout = timeout if timeout is not None else self._timeout
        timeout = int(timeout * 1000)

        #TODO connect timeout? async version?
        self._handle = zookeeper.init(self._hosts, self._session_watcher,
            timeout)
        self._connected_async_result.wait()

    def close(self):
        """Close connection."""
        if self._connected:
            code = zookeeper.close(self._handle)
            self._handle = None
            self._connected = False
            if code != zookeeper.OK:
                raise err_to_exception(code)
        self._event.cancel()
        os.close(self._pipe_read)
        os.close(self._pipe_write)
        
    def add_auth_async(self, scheme, credential):
        async_result = self._new_async_result()

        def callback(handle, code):
            if code != zookeeper.OK:
                exc = err_to_exception(code)
                async_result.set_exception(exc)
            else:
                async_result.set(None)

        zookeeper.add_auth(self._handle, scheme, credential, callback)
        return async_result

    def add_auth(self, scheme, credential):
        return self.add_auth_async(scheme, credential).get()

    def create_async(self, path, value, acl, flags):
        async_result = self._new_async_result()

        def callback(handle, code, path):
            self._queue_result(async_result, code != zookeeper.OK,
                err_to_exception(code) if code != zookeeper.OK else path)

        zookeeper.acreate(self._handle, path, value, acl, flags, callback)
        return async_result

    def create(self, path, value, acl, flags):
        """Create a node.

        @raise zookeeper.NoNodeException: If there's a node missing
            along the path.
        @raise zookeeper.NodeExistsException: The node already existed.
        """
        return self.create_async(path, value, acl, flags).get()

    def get_async(self, path, watcher=None):
        async_result = self._new_async_result()
        watcher_callback, watcher_greenlet = self._setup_watcher(watcher)

        def callback(handle, code, value, stat):
            self._queue_result(async_result, code != zookeeper.OK,
                err_to_exception(code) if code != zookeeper.OK
                    else (value, stat), watcher_greenlet)

        zookeeper.aget(self._handle, path, watcher_callback, callback)
        return async_result

    def get(self, path, watcher=None):
        """Return data and stat for a specific node.

        @raise zookeeper.NoNodeException: If the node did not exist.
        """
        return self.get_async(path, watcher).get()

    def get_children_async(self, path, watcher=None):
        async_result = self._new_async_result()
        watcher_callback, watcher_greenlet = self._setup_watcher(watcher)

        def callback(handle, code, children):
            self._queue_result(async_result, code != zookeeper.OK,
                err_to_exception(code) if code != zookeeper.OK
                    else children, watcher_greenlet)

        zookeeper.aget_children(self._handle, path, watcher_callback, callback)
        return async_result

    def get_children(self, path, watcher=None):
        """Return a list of nodes belong C{path}.

        @raise zookeeper.NoNodeException: If the node did not exist.
        """
        return self.get_children_async(path, watcher).get()

    def set_async(self, path, data, version=-1):
        async_result = self._new_async_result()

        def callback(handle, code, stat):
            self._queue_result(async_result, code != zookeeper.OK,
                err_to_exception(code) if code != zookeeper.OK else stat)

        zookeeper.aset(self._handle, path, data, version, callback)
        return async_result

    def set(self, path, data, version=-1):
        return self.set_async(path, data, version).get()

    def exists_async(self, path, watcher):
        async_result = self._new_async_result()
        watcher_callback, watcher_greenlet = self._setup_watcher(watcher)

        def callback(handle, code, stat):
            if code == zookeeper.NONODE:
                self._queue_result(async_result, False, (False, None),
                    watcher_greenlet)
            else:
                self._queue_result(async_result, code != zookeeper.OK,
                    err_to_exception(code) if code != zookeeper.OK
                       else (True, stat), watcher_greenlet)

        zookeeper.aexists(self._handle, path, watcher_callback, callback)
        return async_result

    def exists(self, path, watcher=None):
        """."""
        return self.exists_async(path, watcher).get()

    def delete_async(self, path, version=-1):
        async_result = self._new_async_result()

        def callback(handle, code):
            self._queue_result(async_result, code != zookeeper.OK,
                err_to_exception(code) if code != zookeeper.OK else code)
            
        zookeeper.adelete(self._handle, path, version, callback)
        return async_result

    def delete(self, path, version=-1):
        """."""
        return self.delete_async(path, version).get()
