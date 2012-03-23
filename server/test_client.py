import contextlib
import random
import simplejson
import socket
import struct
import time

from uuss import proto
from lolapps.util.adapters import chunking

import logging

log = logging.getLogger(__name__)
log.setLevel(logging.DEBUG)
h = logging.StreamHandler()
h.setLevel(logging.DEBUG)
log.addHandler(h)

for i in xrange(0, 1): #random.randint(50, 100)):
    try:
        sock = socket.socket()
        sock.settimeout(None)
        sock.setblocking(1)
        
        #if random.randint(0, 1):
        #    u1 = '658423354'
        #    u2 = '100002097933249'
        #else:
        #    u1 = '100002097933249'
        #    u2 = '658423354'
        
        u1 = random.randint(1000, 100000000000)
        u2 = u1
        # > check so that we can do a double-lock and be sure that multiple concurrent runs won't cause a deadlock
        while u2 == u1 or u2 > u1:
            u2 = random.randint(1000, 100000000000)
        u1 = str(u1)
        u2 = str(u2)
        
        sock.connect(('localhost', 8426))
        with contextlib.closing(sock):
            protocol = proto.UUSSProtocol(sock)

            log.info('Send a message to the local game')
            req = proto.SendMessageRequest()
            req.game = 'dane'
            req.user_id = u2
            req.source_game = 'dane'
            req.source_user_id = u1
            req.message = simplejson.dumps('dane calling dane')
            protocol.send_message(req)
            resp = protocol.recv_message()
            log.info('Message id: %r', resp.message_id)

            log.info('Send a message to a remote game')
            req = proto.SendMessageRequest()
            req.game = 'india'
            req.user_id = u2
            req.source_game = 'dane'
            req.source_user_id = u1
            req.message = simplejson.dumps('dane calling india')
            protocol.send_message(req)
            resp = protocol.recv_message()
            log.info('Message id: %r', resp.message_id)
        
            log.info('Get a userstate with a lock')
            req = proto.GetRequest()
            req.game = 'dane'
            req.user_id = u1
            req.create_if_missing = True
            req.lock = True
            protocol.send_message(req)
            resp = protocol.recv_message()
            state = chunking.reconstitute_chunks(resp.state, True)    
            log.debug("state: %s...", repr(state)[:200])
            log.debug("state['uuss_test']: %r", state.get('uuss_test', None))
            
            log.info('Get another userstate without a lock')
            req = proto.GetRequest()
            req.game = 'dane'
            req.user_id = u2
            req.create_if_missing = True
            protocol.send_message(req)
            resp = protocol.recv_message()
            other_state = chunking.reconstitute_chunks(resp.state, True)
            log.debug("other_state: %s...", repr(other_state)[:200])
            
            log.info('Get another userstate with a lock')
            req = proto.GetRequest()
            req.game = 'dane'
            #req.user_id = u1
            req.user_id = u2
            req.create_if_missing = True
            req.lock = True
            protocol.send_message(req)
            resp = protocol.recv_message()
            other_state = chunking.reconstitute_chunks(resp.state, True)
            log.debug("other_state: %s...", repr(other_state)[:200])
            
            log.info('Release another userstate lock')
            req = proto.ReleaseLock()
            req.game = 'dane'
            req.user_id = u2
            protocol.send_message(req)
            #RECEIVE LockReleased
            resp = protocol.recv_message()
            
            log.info('Save a userstate')
            state.setdefault('uuss_test', {'i': 0})['i'] += 1
            req = proto.SaveRequest()
            req.game = 'dane'
            req.user_id = u1
            req.state = chunking.blow_chunks(state)
            protocol.send_message(req)
            resp = protocol.recv_message()
            
            log.info('Release a userstate lock')
            req = proto.ReleaseLock()
            req.game = 'dane'
            req.user_id = u1
            protocol.send_message(req)
            #RECEIVE LockReleased
            resp = protocol.recv_message()
        
            sock.shutdown(socket.SHUT_RDWR)
    except Exception, e:
        if isinstance(e, KeyboardInterrupt):
            raise
        import traceback
        traceback.print_exc()

