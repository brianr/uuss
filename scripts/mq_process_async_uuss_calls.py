import os

from paste.deploy import converters


log = None

config = None


def handle_item(item):
    from uuss import proto
    log.info("Processing %r", item)
    #return True

    retry = 0
    while retry < 3:
        retry += 1
        try:
            (version, msg) = proto.UUSSProtocol._parse_message(item)
            with proto.get_processor_for_message(msg, version).get_response(None, msg, config) as msg_out:
                log.debug("Message processed %r", msg_out)
            return True
        except Exception, e:
            log.exception("Exception while processing UUSS message from mq on retry %r: %r" % (retry, item))
    return False


def signalhandler(signum, frame):
    from lolapps.util import processor
    processor.signalhandler(signum, frame)
    

def start(args):
    if args.multi:
        import signal
        import sys
        def handle_term(signum, frame):
            print "SIGTERM caught in main process"
            #from uuss import stats
            #stats.stop()
            sys.exit()
        signal.signal(signal.SIGTERM, handle_term)
        import multiprocessing
        procs = []
        for c in range(multiprocessing.cpu_count() - 1):
            print "Starting subprocess %r" % c
            proc = multiprocessing.Process(target=start_one, args=(args,))
            proc.daemon = True
            proc.start()
            procs.append(proc)
        import time
        with open(args.pidfile, 'a') as pidfile:
            pids = {}
            while len(pids) != len(procs):
                time.sleep(0.5)
                for proc in procs:
                    if proc.pid is not None and proc.pid not in pids:
                        pids[proc.pid] = proc.pid
                        pidfile.write("%i\n" % proc.pid)
        
        for proc in procs:
            proc.join()
    else:
        start_one(args)
    

def start_one(args):
    from gevent import monkey
    monkey.patch_all()
    
    from gevent import event
    import threading
    threading.Event = event.Event
    
    import lolapps.common.gevent.mongodb_pool
    
    # Use the gevent socket pool for raidcache and the non-thread-local raidcache client
    from lolapps.util.adapters import raidcache
    raidcache.Client = raidcache.RaidcacheClient
    from lolapps.common.gevent import socket_pool
    raidcache.Pool = socket_pool.SocketPool

    global config

    from uuss.server import configuration
    
    (full_config, config) = configuration.init_from_ini(
        args.ini,
        # override the message sending type (we don't want to put messages back in the mq)
        overrides={ 'uums.send_message_type': 'direct'})

    import logging

    global log
    log = logging.getLogger(__name__)

    log.info('Starting uuss mq processor')

    from uuss.server import model

    import lolapps.util.mq_handler as mq_handler

    import gc
    log.info("GC collected %r objects", gc.collect())

    mq_handler.register_signal_handlers(signalhandler)
    mq_handler.start(1, model.mq, model.MQ_UUSS, handle_item, lambda: None)
    

def main():
    import argparse

    parser = argparse.ArgumentParser(description='Run the UUSS MQ processor.')
    parser.add_argument('-d', '--daemon', action='store_true', help='daemonize')
    parser.add_argument('-i', '--ini', help='ini file', required=True)
    parser.add_argument('-m', '--multi', action='store_true', help='use multiprocessing')
    parser.add_argument('-p', '--pidfile', help='file to use for the pid/lock when daemonizing', default='/tmp/uuss.mq_processor.pid')
    args = parser.parse_args()

    args.ini = os.path.abspath(args.ini)

    if args.daemon:
        import daemon
        import daemon.pidfile
        with daemon.DaemonContext(
            pidfile=daemon.pidfile.TimeoutPIDLockFile(args.pidfile),
            working_directory='/tmp',
            stdout=open('/var/log/lolapps/uuss.mq_processor.stdout', 'w'),
            stderr=open('/var/log/lolapps/uuss.mq_processor.stderr', 'w')
            ):
            start(args)
    else:
        start(args)
    

if __name__ == '__main__':
    main()
