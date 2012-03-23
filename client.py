from binascii import crc32
import contextlib
import inspect
import itertools
import logging
from lolapps.common import uums
from uuss import UserstateException, proto, check_user_id
from lolapps.util.adapters import chunking
from lolapps.util import lolsocket
from paste.deploy import converters
import random
import simplejson
import socket
import os
import thread

log = logging.getLogger(__name__)


LOCK_TIMEOUT = 60
MAX_WAIT_FOR_LOCK = 40


def _autoreconnect(method):
    """
    In the case of any exception while calling the protocol's methods, we will shutdown the connection,
    close the socket, and reraise the exception. When this wrapper is next called a new conneciton
    will be made.
    """
    # NOTE(jpatrin): the userstate parameter here is a UserState object below, equivalent to self in the instance methods
    def wrap(userstate, *a, **kw):
        try:
            if userstate._socket is None:
                log.debug("autoreconnect: %s for %r", ("connecting", "reconnecting")[userstate._disconnected], method)
                userstate._connect()
                if userstate._disconnected:
                    log.debug("autoreconnect: %r for %r", ("connected", "reconnected")[userstate._disconnected], method)
            return method(userstate, *a, **kw)
        except Exception, e:
            log.debug("autoreconnect: exception %r in %r, shutting down the socket", e, method)
            userstate._disconnected = True
            
            import sys
            exc = sys.exc_info()
            try:
                userstate._socket.shutdown(socket.SHUT_RDWR)
            except Exception, e:
                log.debug("Unable to shutdown socket, %r", e)
            try:
                userstate._socket.close()
            except Exception, e:
                log.debug("Unable to close socket, %r", e)
            userstate._socket = None
            from lolapps.util import reraise
            reraise.reraise(exc)
    return wrap


# TODO(jpatrin): The thread-local here should make each thread have its own UUSS connection
#   This should probably be removed in favor of a pooled approach
import threading
class UserState(threading.local):
    is_remote = property(lambda self: True)

    def __init__(
        self,
        config,
        game,
        chunk_config={},
    ):
        """
        """
        log.debug("uuss.UserState.__init__")
        self._config = config
        self._app_id = self._config['app_id']
        self.game = game
        self.chunk_config = chunk_config

        self._lazy_load_chunks = converters.asbool(self._config.get('user_state.lazy_load_chunks', False))

        # supported syntax is:
        # user_state.uuss_server = host1:42-49,host2:1111-2000,host3:8765
        servers = []
        for serverspec in converters.aslist(self._config['user_state.uuss_server'], ','):
            (host, portspec) = serverspec.split(':')
            ports = map(int, portspec.split('-'))
            if len(ports) == 1:
                minport = ports[0]
                maxport = minport
            else:
                (minport, maxport) = ports
            for port in range(minport, maxport + 1):
                servers.append( (host, port) )
        self._servers = servers
        
        self._socket = None
        self._disconnected = False
        self._ping_counter = 0

    def _connect(self):
        # Init net connection to UUSS
        self._socket = lolsocket.LolSocket(socket.socket())
        #self._socket.settimeout(None)
        self._socket.setblocking(1)

        hash_index = crc32("%s_%s" % (socket.gethostname(), self._config.get('global_conf', {}).get('http_port', random.random()))) % len(self._servers)
        server = self._servers[hash_index]
        log.debug("Connecting to %r", server)
        self._socket.connect(server)
        self.protocol = proto.UUSSProtocol(self._socket)

    @_autoreconnect
    def _protocol_send_message(self, *a, **kw):
        return self.protocol.send_message(*a, **kw)

    @_autoreconnect
    def _recv_expected_message(self, *a, **kw):
        return self.protocol.recv_expected_message(*a, **kw)[1]

    @_autoreconnect
    def _recv_expected_message_class(self, *a, **kw):
        return self.protocol.recv_expected_message_class(*a, **kw)[1]

    ## Public API for UUSS/UserState
    @contextlib.contextmanager
    def open(self, user_id, lock_timeout=None, max_wait=None, label='generic', create_if_missing=True):
        """
        Context manager (use with the 'with' statement) that provides safe
        access to a user's state. Usage:
        
        with userstate.open(user_id, label='unique_id') as state:
            # ... do stuff, possibly calling userstate.save(user_id, state)
    
        Holds the lock for at most `lock_timeout` seconds. If the lock is not acquired within `max_wait` seconds
        of beginning to try acquiring it, we raise an exception.

        If `lock_timeout` is None, the default in LOCK_TIMEOUT is used.
        If `max_wait` is None, the default in MAX_WAIT_FOR_LOCK is used.
        """

        if label == 'generic':
            try:
                for caller in inspect.stack():
                    if str(caller[3]) != '__enter__':
                        break
                label = "%s_%s" % ('/'.join(caller[1].split('/')[-2:]), caller[3])
                log.warning("[w:generic_lock_label] %r should be changed to pass a useful lock label in %r line %r", caller[3], caller[1], caller[2])
            except:
                pass
        label += "_%s-%s-%s" % (socket.gethostname().split('.')[0], os.getpid(), thread.get_ident())
    
        user_id = str(user_id)

        if max_wait is None:
            max_wait = MAX_WAIT_FOR_LOCK

        if lock_timeout is None:
            lock_timeout = LOCK_TIMEOUT

        log.debug("uuss.UserState.open %r", user_id)

        # NOTE(jpatrin): We're using get() here so that extensions of get() can still process
        # the returned state (or do other things), such as dane.lib.userstate.UserState.get()
        kw = {
            '_lock_timeout': lock_timeout,
            '_lock_max_wait': max_wait,
            '_lock_label': label,
            '_lock': True
            }
        try:
            yield self.get(user_id, create_if_missing=create_if_missing, **kw)
        finally:
            req = proto.ReleaseLock()
            req.game = self.game
            req.user_id = user_id
            self._protocol_send_message(req)
            resp = self._recv_expected_message(proto.LockReleased, self.game, user_id)

    def get(self, user_id, create_if_missing=False, **_private_kw):
        """
        Get state for the specified user, but don't acquire a lock.
        Returns None if the state was not found.

        Note: _private_kw should never be used as part of the public API.
        It is a private interface which allows open() to acquire a lock
        and is UNSAFE for external use.
        """
        log.debug("uuss.UserState.get %r", user_id)
        user_id = str(user_id)

        if not user_id:
            raise UserstateException("user_id is empty... %r" % user_id)
        if user_id[0] == "{":
            raise UserstateException("user_id appears to be json... %r" % user_id)

        req = proto.GetRequest()
        req.game = self.game
        req.user_id = user_id
        req.create_if_missing = create_if_missing
        req.lock = _private_kw.get('_lock', False)
        req.lock_label = _private_kw.get('_lock_label', 'generic')
        req.lock_timeout = _private_kw.get('_lock_timeout', 60)
        req.lock_max_wait = _private_kw.get('_lock_max_wait', 30)
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.GetResponse, self.game, user_id)

        if resp.state == "":
            state = None
        else:
            state = chunking.reconstitute_chunks(resp.state, self._lazy_load_chunks)
            check_user_id(user_id, state, game=self.game)

        return state

    def save(self, user_id, state):
        """
        Save the state for the specified user (but don't release the lock).
        user_id: string
        state: dict (not a json string)
        """
        log.debug("uuss.UserState.save %r", user_id)
        user_id = str(user_id)
        # Debugging for FB20021
        if user_id == 'null':
            raise Exception('"null" user_id in userstate.save')
        if not isinstance(state, dict):
            raise Exception('state not a dict for user_id %s' % user_id)

        check_user_id(user_id, state, game=self.game)

        req = proto.SaveRequest()
        req.game = self.game
        req.user_id = user_id
        req.state = chunking.blow_chunks(state, self.chunk_config)
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.SaveResponse, self.game, user_id)

    def delete(self, user_id):
        """
        Delete the state for the specified user. This internally requires a lock, so don't put
        calls to this inside of a with userstate.open block.
        user_id: string
        """
        log.debug("uuss.UserState.delete %r", user_id)
        user_id = str(user_id)

        req = proto.DeleteRequest()
        req.game = self.game
        req.user_id = user_id
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.DeleteResponse, self.game, user_id)


    ## Public UUMS interface, note that the dest_game is implied since a UserState instance is already tied to a game
    def get_messages(self, user_id):
        """
        Messages will be reutrned in a list. Note that the message which was passed into
        send_message will be in the 'message' key of the returned message.
        """
        log.debug("uuss.UserState.get_messages Getting messages for %r in %r", user_id, self.game)
        user_id = str(user_id)

        req = proto.GetMessagesRequest()
        req.game = self.game
        req.user_id = user_id
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.GetMessagesResponse, self.game, user_id)
        return [simplejson.loads(m) for m in resp.messages]

    def _send_message(
        self,
        source_game,
        dest_game,
        source_user_id,
        dest_user_id,
        message,
        priority=uums.MUST_PERSIST
        ):
        log.debug(
            'uuss.UserState.get Sending message from %r to %r for %r to %r with %r priority: %r',
            source_game, dest_game, source_user_id, dest_user_id, priority, message)
        
        source_user_id = str(source_user_id)
        dest_user_id = str(dest_user_id)

        req = proto.SendMessageRequest()
        req.game = dest_game
        req.user_id = dest_user_id
        req.source_game = source_game
        req.source_user_id = source_user_id
        req.message = simplejson.dumps(message)
        req.priority = priority
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.SendMessageResponse, dest_game, dest_user_id)
        return resp.message_id

    # TODO(jpatrin): add a send_messages?
    # For use when self.game is the current game (i.e. pasters)
    def send_message_to(
        self,
        dest_game,
        source_user_id,
        dest_user_id,
        message,
        priority=uums.MUST_PERSIST
        ):
        return self._send_message(
            self.game,
            dest_game,
            source_user_id,
            dest_user_id,
            message,
            priority=uums.MUST_PERSIST)

    # For use when self.game is the game we want to send to (i.e. from one UUSS server to another)
    def send_message_from(
        self,
        source_game,
        source_user_id,
        dest_user_id,
        message,
        priority=uums.MUST_PERSIST
        ):
        return self._send_message(
            source_game,
            self.game,
            source_user_id,
            dest_user_id,
            message,
            priority=uums.MUST_PERSIST)

    def remove_messages(self, user_id, message_ids):
        log.debug('uuss.UserState.remove_messages Removing messages %r from %r for %r', message_ids, self.game, user_id)
        user_id = str(user_id)

        req = proto.RemoveMessagesRequest()
        req.game = self.game
        req.user_id = user_id
        req.message_ids.extend(message_ids)
        self._protocol_send_message(req)
        resp = self._recv_expected_message(proto.RemoveMessagesResponse, self.game, user_id)

    def ping(self):
        log.debug('uuss.UserState.ping')

        req = proto.Ping()
        req.counter = self._ping_counter
        self._protocol_send_message(req)
        resp = self._recv_expected_message_class(proto.Pong)
        if resp.counter != req.counter:
            raise UserstateException("Ping counter in response (%r) does not match request (%r)", resp.counter, req.counter)
        self._ping_counter += 1
        return resp.counter
