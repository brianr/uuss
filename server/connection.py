from gevent import GreenletExit

import socket
import traceback

from lolapps.common.userstate_multi import UserstateLockedException
from lolapps.util import lolsocket
from uuss import proto

import logging

log = logging.getLogger(__name__)


class ConnectionHandler(object):
    def __init__(self, socket, address, config=None):
        self.socket = socket
        self.socket.settimeout(None)
        self.socket.setblocking(1)
        self.address = address
        self.config = config or {}
        self._go = True
        self.protocol = proto.UUSSProtocol(self.socket, self.config)

    def run(self):
        try:
            while self._go:
                log.debug("self._go is %r", self._go)
                (version, msg) = self.protocol.recv_message()
                log.debug("Getting response in ConnectionHandler.run")
                with proto.get_processor_for_message(msg, version).get_response(self.protocol, msg, self.config) as msg_out:
                    log.debug("Sending response in ConnectionHandler.run")
                    self.protocol.send_message(msg_out, version)
            log.debug("Shutdown acknowledged for %r", self.address)
                    
        except lolsocket.ConnectionClosedException, e:
            # ignore, this is expected from time to time
            log.debug("Connection from %r closed", self.address)
        except GreenletExit, e:
            log.debug("ConnectionHandler caught GreenletExit for %r", self.address)
        except proto.UUSSFailHealthcheckException, e:
            try:
                exmsg = proto.ExceptionResponse()
                exmsg.message = "Failing healthcheck"
                exmsg.traceback = ""
                self.protocol.send_message(exmsg)
            except:
                #ignore errors that may happen when sending an exception message
                pass
        except UserstateLockedException, e:
            log.warning(repr(e))
            try:
                exmsg = proto.ExceptionResponse()
                exmsg.message = repr(e)
                exmsg.traceback = traceback.format_exc()
                self.protocol.send_message(exmsg)
            except:
                #ignore errors that may happen when sending an exception message
                pass
        except Exception, e:
            import sys
            exc = sys.exc_info()
            tb = traceback.format_exc()

            # try to send an exception message to let the client know what happened
            # NOTE(jpatrin): if the exception happened in the middle of receiving
            #  or sending a message, this will likely be lost
            try:
                exmsg = proto.ExceptionResponse()
                exmsg.message = repr(e)
                exmsg.traceback = traceback.format_exc()
                self.protocol.send_message(exmsg)
            except:
                #ignore errors that may happen when sending an exception message
                pass

            # reraise the exception, allowing the socket to be closed and this
            #  greenlet (thread) to die
            log.error("Exception in ConnectionHandler (%r)\n%s", e, tb)
            from lolapps.util import reraise
            reraise.reraise(exc)
        finally:
            try:
                log.debug("Socket shutdown for %r", self.address);
                self.socket.shutdown(socket.SHUT_RDWR)
            except Exception, e:
                log.debug("Unable to shutdown socket, oh well, we're quitting anyway")
            try:
                log.debug("Socket close for %r", self.address);
                self.socket.close()
            except Exception, e:
                log.debug("Unable to close socket, oh well, we're quitting anyway")
            del self.socket
            log.debug("ConnectionHandler.run exiting for %r", self.address)

    def shutdown(self):
        log.debug("shutdown received for %r", self.address)
        self._go = False

    def fail_healthcheck(self, fail=True):
        log.debug("fail_healthcheck (%r) received for %r", fail, self.address)
        self.protocol.fail_healthcheck = fail
