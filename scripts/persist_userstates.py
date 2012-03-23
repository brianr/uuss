"""
"""
import logging
import multiprocessing
import optparse
import os
import os.path
import sys
import time

import sqlalchemy as sa

# setup the loggers
log = None

BATCH_SIZE = 5000
#NUM_PROCS = multiprocessing.cpu_count() * 5
NUM_PROCS = 4

def main():
    #i = 14074449
    go = True
    counter = 0
    not_in_memcache = 0
    while go:
        pool = multiprocessing.Pool(NUM_PROCS)
        pool.map(run_batch, [i * BATCH_SIZE for i in xrange(56000000 / BATCH_SIZE, 100000000 / BATCH_SIZE)], 1)
    log.info("DONE, i = %r, processed = %r, not_in_memcache = %r" % (i, counter, not_in_memcache))

def run_batch(i):
    engine, userstate = configure()

    import gc
    log.info("GC collected %r objects", gc.collect())
    
    counter = 0
    not_in_memcache = 0

    log.info("starting run, i = %r, BATCH_SIZE = %r" % (i, BATCH_SIZE))
    stmt = sa.text("""SELECT user_id FROM user_state LIMIT %r OFFSET %r""" % (BATCH_SIZE, i))
    resultset = engine.execute(stmt)
    prev_time = time.time()

    go = False
    for row in resultset:
        go = True
        i += 1
        try:
            user_id = row['user_id']
            log.info("i = %r, processed = %r, user_id = %r, not_in_memcache = %r" % 
                     (i, counter, user_id, not_in_memcache))
            try:
                userstate.backup(user_id)
            except:
                not_in_memcache += 1
                continue
            with userstate.open(user_id) as state:
                userstate.save(user_id, state)
        except Exception, e:
            log.exception("exception processing %s" % user_id)

        counter += 1

        proc_time = time.time() - prev_time
        log.info("i = %r, processed = %r, time = %r, user_id = %r, not_in_memcache = %r" % 
                 (i, counter, proc_time, user_id, not_in_memcache))
        prev_time = time.time()

def configure():
    import argparse

    parser = argparse.ArgumentParser(description='Run the UUSS server.')

    parser.add_argument("-d", "--shard", help="shard to process", type=int, required=True)
    parser.add_argument("-i", "--ini", help="ini file", required=True)
    parser.add_argument("-g", "--game", help="game file", required=True)
    args = parser.parse_args()

    from uuss.server import configuration
    (full_config, config) = configuration.init_from_ini(args.ini)

    global log
    log = logging.getLogger(__name__)

    from uuss.server import model

    game_model = getattr(model, args.game)

    return game_model.userstate_engines["userstate_shard_%s" % args.shard], game_model.userstate

if __name__ == '__main__':
    main()
