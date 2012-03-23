import os

from paste.deploy import converters


HEALTHCHECK_FAIL_FILE = '/tmp/healthcheck_fail.txt'
SHUTDOWN_FILE = '/tmp/shutdown.txt'

log = None

config = None

fail_healthcheck_event = None
unset_fail_healthcheck_event = None
shutdown_event = None
restart_or_die_event  = None

die = False

server = None


def handle(socket, address):
    log.debug('new connection from %r', address)
    
    from uuss.server.connection import ConnectionHandler
    connection_handler = ConnectionHandler(socket, address, config)

    from gevent import Greenlet
    
    fail_healthcheck_watcher = Greenlet.spawn(connection_watch_for_fail_healthcheck, connection_handler)
    unset_fail_healthcheck_watcher = Greenlet.spawn(connection_watch_for_unset_fail_healthcheck, connection_handler)
    shutdown_watcher = Greenlet.spawn(connection_watch_for_shutdown, connection_handler)

    # make sure that if the events are set the connection_handler uses them right away
    # (the greenlets watcher greenlets won't be run until after this handler has started processing)
    if fail_healthcheck_event.wait(0):
        connection_handler.fail_healthcheck()
    if unset_fail_healthcheck_event.wait(0):
        connection_handler.fail_healthcheck(False)
    if shutdown_event.wait(0):
        connection_handler.shutdown()
    
    try:
        connection_handler.run()
    finally:
        fail_healthcheck_watcher.kill()
        unset_fail_healthcheck_watcher.kill()
        shutdown_watcher.kill()
        del connection_handler


def connection_watch_for_shutdown(connection_handler):
    shutdown_event.wait()
    connection_handler.shutdown()


def connection_watch_for_fail_healthcheck(connection_handler):
    fail_healthcheck_event.wait()
    connection_handler.fail_healthcheck()


def connection_watch_for_unset_fail_healthcheck(connection_handler):
    unset_fail_healthcheck_event.wait()
    connection_handler.fail_healthcheck(False)


def stop():
    from gevent import GreenletExit
    try:
        server.stop()
    except GreenletExit:
        pass


def start(args):
    if args.multi:
        import signal
        import sys
        def handle_term(signum, frame):
            print "SIGTERM caught in main process"
            global die
            die = True
            from uuss import stats
            stats.stop()
            sys.exit()
        signal.signal(signal.SIGTERM, handle_term)
        import multiprocessing
        procs = []
        for c in range(multiprocessing.cpu_count() - 1):
            print "Starting subprocess %r" % c
            proc = multiprocessing.Process(target=start_one, args=(args, c))
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


def start_one(args, num=0):
    import signal
    import sys
    def handle_term(signum, frame):
        log.warn("SIGTERM caught in worker process")
        global die
        die = True

        shutdown_event.set()
        restart_or_die_event.set()

        from uuss import stats
        stats.stop()
        
        global server
        if server:
            stop()
            server = None
    signal.signal(signal.SIGTERM, handle_term)

    from gevent import monkey
    monkey.patch_all()

    import lolapps.common.gevent.mongodb_pool
    
    # Use the gevent socket pool for raidcache and the non-thread-local raidcache client
    from lolapps.util.adapters import raidcache
    raidcache.Client = raidcache.RaidcacheClient
    from lolapps.common.gevent import socket_pool
    raidcache.Pool = socket_pool.SocketPool

    global config

    from uuss.server import configuration
    
    (full_config, config) = configuration.init_from_ini(args.ini)

    import logging

    global log
    log = logging.getLogger(__name__)

    from uuss import stats
    from gevent import event
    stats.start(event.Event)

    log.info('Starting uuss server')

    global fail_healthcheck_event, unset_fail_healthcheck_event, shutdown_event, restart_or_die_event
    fail_healthcheck_event = event.Event()
    unset_fail_healthcheck_event = event.Event()
    shutdown_event = event.Event()
    restart_or_die_event = event.Event()

    from gevent import Greenlet
    Greenlet.spawn(watch_for_healthcheck_fail)
    Greenlet.spawn(watch_for_shutdown)

    import gc
    log.debug("GC collected %r objects", gc.collect())

    start_server(num)
        

def start_server(num):
    global server
    
    from gevent.server import StreamServer
    from gevent.pool import Pool as Group
    
    while True:
        port = converters.asint(config.get('uuss_port')) + num
        server = StreamServer(('0.0.0.0', port), handle, spawn=Group())

        log.info("UUSS server ready on port %r", port)
        server.serve_forever()
        
        log.info("Server exited")
        if die:
            break
        restart_or_die_event.wait()
        if die:
            break
        log.info("Server restarting")
    log.info("Exiting")


def watch_for_healthcheck_fail():
    import os
    import time
    failed = False
    while True:
        if os.path.exists(HEALTHCHECK_FAIL_FILE):
            if not failed:
                log.warn('%s exists', HEALTHCHECK_FAIL_FILE)
                unset_fail_healthcheck_event.clear()
                fail_healthcheck_event.set()
                failed = True
        elif failed:
            log.warn('%s removed', HEALTHCHECK_FAIL_FILE)
            fail_healthcheck_event.clear()
            unset_fail_healthcheck_event.set()
            failed = False
        time.sleep(1)
    
def watch_for_shutdown():
    import os
    import time
    accepting = True
    while True:
        if os.path.exists(SHUTDOWN_FILE):
            if accepting:
                restart_or_die_event.clear()
                shutdown_event.set()
                log.warn('%s exists', SHUTDOWN_FILE)
                global server
                if server:
                    stop()
                    server = None
                accepting = False
        elif not accepting:
            shutdown_event.clear()
            log.warn('%s removed', SHUTDOWN_FILE)
            restart_or_die_event.set()
            accepting = True
        time.sleep(1)


def main():
    import argparse

    parser = argparse.ArgumentParser(description='Run the UUSS server.')
    parser.add_argument('-d', '--daemon', action='store_true', help='daemonize')
    parser.add_argument('-i', '--ini', help='ini file', required=True)
    parser.add_argument('-m', '--multi', action='store_true', help='use multiprocessing')
    parser.add_argument('-p', '--pidfile', help='file to use for the pid/lock when daemonizing', default='/tmp/uuss.pid')
    args = parser.parse_args()

    args.ini = os.path.abspath(args.ini)

    if args.daemon:
        import daemon
        import daemon.pidfile
        with daemon.DaemonContext(
            pidfile=daemon.pidfile.TimeoutPIDLockFile(args.pidfile),
            working_directory='/tmp',
            stdout=open('/var/log/lolapps/uuss.stdout', 'w'),
            stderr=open('/var/log/lolapps/uuss.stderr', 'w')
            ):
            start(args)
    else:
        start(args)


if __name__ == '__main__':
    main()
