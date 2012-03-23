import contextlib
from uuss.server import model
from lolapps.common import uums
from lolapps.util import json
import os
import simplejson
import struct
import time

try:
    # Try importing the C++ extension version
    import uuss_pb
except:
    # The dynamic python version will automatically be used
    pass
from uuss.uuss_pb2 import *

from uuss.server import model
from lolapps.common import uums

from lolapps.util.adapters import chunking
from lolapps.util import lolsocket

import logging

log = logging.getLogger(__name__)


class UUSSProtocolException(Exception):
    pass


class UUSSShardDownException(UUSSProtocolException):
    pass


class UUSSFailHealthcheckException(UUSSProtocolException):
    pass


class UUSSAction(object):
    """
    Base class for UUSS actions.

    Note: the get_response and _call methods are set as @contextlib.contextmanager
    in order for a get-with-lock to be able to use the with userstate.open mechanism
    to ensure that the lock is released at the appropriate time.
    """
    def __init__(self):
        self.Request = None
        self.Response = None

    @contextlib.contextmanager
    def get_response(self, protocol, req, config):
        log.debug("UUSSAction.get_response start (%r, %r)", req.user_id, req.game)
        userstate = getattr(model, req.game).userstate
        log.debug("UUSSAction.get_response userstate: %r", userstate)
        with self._call(protocol, userstate, req, config) as resp:
            assert req.user_id == resp.user_id
            assert req.game == resp.game

            log.debug("UUSSAction.get_response pre-yield (%r, %r)", req.user_id, req.game)
            yield resp
            log.debug("UUSSAction.get_response post-yield (%r, %r)", req.user_id, req.game)

        log.debug("UUSSAction.get_response end (%r, %r)", req.user_id, req.game)

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        raise Exception('Implement me!')


## UUSS (UserState) Protocol ##
class Get(UUSSAction):
    def __init__(self):
        self.Request = GetRequest
        self.Response = GetResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("Get._call start (%r, %r)", req.user_id, req.game)
        if req.lock:
            # If a lock is requested we need to keep control within this 'with' block
            # until we receive a ReleaseLock message but we also need to "return"
            # the userstate requested. This is why this method and UUSSAction.get_response
            # are contextmanagers. 'yield' allows us to return the value without
            # leaving the 'with' block.
            with userstate.open(
                req.user_id,
                create_if_missing=req.create_if_missing,
                lock_timeout=req.lock_timeout,
                max_wait=req.lock_max_wait,
                label=req.lock_label,
                raw=True
            ) as (state, chunked):

                log.debug("Get._call pre-yield (%r, %r)", req.user_id, req.game)
                yield self._build_response(state, chunked, req.game, req.user_id)
                log.debug("Get._call post-yield (%r, %r)", req.user_id, req.game)

                # we require a ReleaseLock message before we can leave this context and release the lock
                self._wait_for_release_lock(protocol, req, config)
        else:
            (state, chunked) = userstate.get(req.user_id, req.create_if_missing, raw=True)

            log.debug("Get._call pre-yield (%r, %r)", req.user_id, req.game)
            yield self._build_response(state, chunked, req.game, req.user_id)
            log.debug("Get._call post-yield (%r, %r)", req.user_id, req.game)

        log.debug("Get._call end (%r, %r)", req.user_id, req.game)

    def _wait_for_release_lock(self, protocol, get_req, config):
        """
        Same as the normal server loop except that we will break once we get and process a ReleaseLock message.

        @see server.connection.ConnectionHandler.run
        """
        log.debug("Get._wait_for_release_lock start (%r, %r)", get_req.user_id, get_req.game)
        while True:
            log.debug("Get._wait_for_release_lock loop (%r, %r)", get_req.user_id, get_req.game)
            (version, req) = protocol.recv_message()

            if req.__class__ is ReleaseLock:
                if req.game != get_req.game:
                    raise UUSSProtocolException("ReleaseLock.game (%r) != GetRequest.game (%r)" % (req.game, get_req.game))
                if req.user_id != get_req.user_id:
                    raise UUSSProtocolException("ReleaseLock.user_id (%r) != GetRequest.user_id (%r)" % (req.user_id, get_req.user_id))
            with get_processor_for_message(req, version).get_response(protocol, req, config) as resp:
                protocol.send_message(resp, version)
            if req.__class__ is ReleaseLock:
                log.debug("Get._wait_for_release_lock end (%r, %r)", get_req.user_id, req.game)
                return

    def _build_response(self, state, chunked, game, user_id):
        if not chunked:
            # get the state in a chunked format for sending along the wire
            # there will be only a master chunk with no chunk config specified
            state = chunking.blow_chunks(state)
        resp = self.Response()
        resp.game = game
        resp.user_id = user_id
        if state is None:
            resp.state = ""
        else:
            resp.state = state
        return resp


class Save(UUSSAction):
    def __init__(self):
        self.Request = SaveRequest
        self.Response = SaveResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("Save._call start (%r, %r)", req.user_id, req.game)
        userstate.save(req.user_id, req.state)
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id

        yield resp

        log.debug("Save._call end (%r, %r)", req.user_id, req.game)


class Lock(UUSSAction):
    def __init__(self):
        self.Request = ReleaseLock
        self.Response = LockReleased

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("Lock._call start (%r, %r)", req.user_id, req.game)
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id
        log.debug("Lock._call pre-yield (%r, %r)", req.user_id, req.game)

        yield resp

        log.debug("Lock._call post-yield (%r, %r)", req.user_id, req.game)
        log.debug("Lock._call end (%r, %r)", req.user_id, req.game)


class Delete(UUSSAction):
    def __init__(self):
        self.Request = DeleteRequest
        self.Response = DeleteResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        if userstate.is_remote:
            raise UUSSProtocolException(
                "DeleteRequest sent for user_id %r game %r but that game is remote. I will only delete userstates in my local games."
                % (req.user_id, req.game))
        with userstate.open(req.user_id, label='UUSS.Delete') as state:
            log.warn("[w:delete_userstate] Deleting userstate for user_id %r game %r, state follows\n%s", req.user_id, req.game, json.dumps(state))
            userstate.delete(req.user_id)
        
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id
        yield resp


## UUMS protocol ##
class GetMessages(UUSSAction):
    def __init__(self):
        self.Request = GetMessagesRequest
        self.Response = GetMessagesResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("GetMessages._call start (%r, %r)", req.user_id, req.game)
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id
        resp.messages.extend([ simplejson.dumps(m) for m in userstate.get_messages(req.user_id) ])
        log.debug("GetMessages._call pre-yield (%r, %r)", req.user_id, req.game)

        yield resp

        log.debug("GetMessages._call post-yield (%r, %r)", req.user_id, req.game)
        log.debug("GetMessages._call end (%r, %r)", req.user_id, req.game)


class SendMessage(UUSSAction):
    def __init__(self):
        self.Request = SendMessageRequest
        self.Response = SendMessageResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("SendMessage._call start (%r, %r)", req.user_id, req.game)

        if config.get('uums.send_message_type', 'direct') == 'mq':
            log.debug("Using mq")
            if not req.message_id:
                req.message_id = uums.new_message_id()
            msg_id = req.message_id
            model.mq.send(model.MQ_UUSS, ''.join(UUSSProtocol._encode_message(req)))
        else:
            send_message = userstate.send_message if hasattr(userstate, 'send_message') else userstate.send_message_from
            msg_id = send_message(
                req.source_game,
                req.source_user_id,
                req.user_id,
                simplejson.loads(req.message),
                req.priority)
        
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id
        resp.message_id = msg_id
        log.debug("SendMessage._call pre-yield (%r, %r)", req.user_id, req.game)

        yield resp

        log.debug("SendMessage._call post-yield (%r, %r)", req.user_id, req.game)
        log.debug("SendMessage._call end (%r, %r)", req.user_id, req.game)


class RemoveMessages(UUSSAction):
    def __init__(self):
        self.Request = RemoveMessagesRequest
        self.Response = RemoveMessagesResponse

    @contextlib.contextmanager
    def _call(self, protocol, userstate, req, config):
        log.debug("RemoveMessages._call start (%r, %r)", req.user_id, req.game)

        userstate.remove_messages(req.user_id, req.message_ids)
        
        resp = self.Response()
        resp.game = req.game
        resp.user_id = req.user_id
        log.debug("RemoveMessages._call pre-yield (%r, %r)", req.user_id, req.game)

        yield resp

        log.debug("RemoveMessages._call post-yield (%r, %r)", req.user_id, req.game)
        log.debug("RemoveMessages._call end (%r, %r)", req.user_id, req.game)


## Ping!
class PingPong(UUSSAction):
    def __init__(self):
        self.Request = Ping
        self.Response = Pong

    @contextlib.contextmanager
    def get_response(self, protocol, req, config):
        log.debug("Ping._call start (%r)", req.counter)
        resp = self.Response()
        resp.counter = req.counter
        log.debug("Ping._call pre-yield (%r)", req.counter)

        if protocol.fail_healthcheck:
            raise UUSSFailHealthcheckException("Failing healthcheck")

        yield resp

        log.debug("Ping._call post-yield (%r)", req.counter)
        log.debug("Ping._call end (%r)", req.counter)


MSG_PROCESSORS = {
    3: [
        Get(),
        Save(),
        Lock(),
        Delete(),
        GetMessages(),
        SendMessage(),
        RemoveMessages(),
        PingPong()
    ],
    2: [
        Get(),
        Save(),
        Lock(),
        GetMessages(),
        SendMessage(),
        RemoveMessages(),
        PingPong()
    ]
}

MSG_TYPES = dict(
    [(version, ([t.Request for t in processors] + [t.Response for t in processors] + [ExceptionResponse]))
     for version, processors in MSG_PROCESSORS.iteritems()]
)

MSG_TYPES_LOOKUP = dict(
    [(version, dict(zip(msg_types, range(len(msg_types)))))
     for version, msg_types in MSG_TYPES.iteritems()]
)

MSG_TYPES_PROCESSOR_LOOKUP = dict(
    [(version, dict([(p.Request, p) for p in processors] +
                    [(p.Response, p) for p in processors]))
     for version, processors in MSG_PROCESSORS.iteritems()]
)

log.debug("MSG_TYPES: %r", MSG_TYPES)
log.debug("MSG_TYPES_LOOKUP: %r", MSG_TYPES_LOOKUP)
log.debug("MSG_TYPES_PROCESSOR_LOOKUP: %r", MSG_TYPES_PROCESSOR_LOOKUP)

VERSION_HEADER_FORMAT = '!H'
VERSION_HEADER_LENGTH = struct.calcsize(VERSION_HEADER_FORMAT)

# The first value must always be H and be the version
HEADER_FORMAT = {
    # Version, Message type, Data length
    2: '!BL',
    # Version, Message type, Data length
    3: '!BL'
}
HEADER_LENGTH = dict(
    [(version, struct.calcsize(header_format))
     for version, header_format in HEADER_FORMAT.iteritems()]
)

VERSION = sorted(HEADER_FORMAT.keys())[-1]

VERSIONS = HEADER_FORMAT.keys()


def get_processor_for_message(msg, version=VERSION):
    return MSG_TYPES_PROCESSOR_LOOKUP[version][msg.__class__]


class UUSSProtocol(object):
    def __init__(self, socket, config=None):
        if isinstance(socket, lolsocket.LolSocket):
            self.socket = socket
        else:
            self.socket = lolsocket.LolSocket(socket)
        self.config = config or {}
        self.fail_healthcheck = False

    @staticmethod
    def _parse_version_header(data):
        (version,) = struct.unpack(VERSION_HEADER_FORMAT, data[:VERSION_HEADER_LENGTH])
        log.debug("Received message version: %r", version)
        if version not in VERSIONS:
            raise UUSSProtocolException("Message version received is %r, we expected one of %r" % (version, VERSIONS))
        remaining_data = data[VERSION_HEADER_LENGTH:]
        return (version, remaining_data)

    @staticmethod
    def _parse_message_header(data, version):
        header_length = HEADER_LENGTH[version]
        (msg_type, msg_len) = struct.unpack(HEADER_FORMAT[version], data[:header_length])
        log.debug("Received message header: %r, %r", msg_type, msg_len)
        try:
            msg_class = MSG_TYPES[version][msg_type]
        except IndexError, e:
            raise UUSSProtocolException("Invalid message type received: %r" % msg_type)
        msg_data = data[header_length:]
        if len(msg_data) != 0 and len(msg_data) != msg_len:
            raise UUSSProtocolException("Length of data (%r) does not match the length set in the header (%r)" % (len(msg_data), msg_len))
        return (msg_type, msg_len, msg_data)

    @staticmethod
    def _parse_message_body(msg_type, data, version):
        #log.debug("data: %r", data)
        msg = MSG_TYPES[version][msg_type]()
        msg.ParseFromString(data)
        if msg.__class__ is ExceptionResponse:
            if 'ShardDown' in msg.message:
                raise UUSSShardDownException("The shard for this userstate is down: %r\n%s" % (msg.message, msg.traceback))
            else:
                raise UUSSProtocolException("Exception received from UUSS server: %r\n%s" % (msg.message, msg.traceback))
        return msg

    @staticmethod
    def _parse_message(data):
        (version, remaining_data) = UUSSProtocol._parse_version_header(data)
        (msg_type, msg_len, msg_data) = UUSSProtocol._parse_message_header(remaining_data, version)
        return (version, UUSSProtocol._parse_message_body(msg_type, msg_data, version))

    def _recv_version_header(self):
        data = self.socket.recv_bytes(VERSION_HEADER_LENGTH)
        return self._parse_version_header(data)[0]

    def _recv_message_header(self, version):
        data = self.socket.recv_bytes(HEADER_LENGTH[version])
        return self._parse_message_header(data, version)[:-1]

    def _recv_message_body(self, msg_type, msg_len):
        # XXX(jpatrin): why does this not parse like the above 2?
        return self.socket.recv_bytes(msg_len)

    @staticmethod
    def _encode_message(msg, version=VERSION):
        data = msg.SerializeToString()
        msg_type = MSG_TYPES_LOOKUP[version][msg.__class__]
        header = struct.pack(VERSION_HEADER_FORMAT, version) + struct.pack(HEADER_FORMAT[version], msg_type, len(data))
        return (header, data)

    ## public interface
    def send_message(self, msg, version=VERSION):
        self.socket.send_bytes(''.join(self._encode_message(msg, version)))

    def recv_expected_message_class(self, expected_msg_class):
        (version, msg) = self.recv_message()
        if msg.__class__ is not expected_msg_class:
            raise UUSSProtocolException("Message class %r expected, got %r instead: %r" % (expected_msg_class, msg.__class__, msg))
        return (version, msg)
        
    def recv_expected_message(self, expected_msg_class, expected_game, expected_user_id):
        (version, msg) = self.recv_expected_message_class(expected_msg_class)
        if msg.game != expected_game:
            raise UUSSProtocolException("Game %r expected, got %r instead" % (expected_game, msg.game))
        if msg.user_id != expected_user_id:
            raise UUSSProtocolException("User ID %r expected, got %r instead" % (expected_user_id, msg.user_id))
        return (version, msg)
    
    def recv_message(self):
        version = self._recv_version_header()
        (msg_type, msg_len) = self._recv_message_header(version)
        log.debug("msg_type: %r", msg_type)
        log.debug("msg_len: %r", msg_len)
        data = self._recv_message_body(msg_type, msg_len)
        return (version, self._parse_message_body(msg_type, data, version))
