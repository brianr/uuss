"""
Script to move userstates from old db shard to new db shard while
rebalancing a sharded userstate setup, after a new db shard has 
been added.
"""
import logging
import optparse
import os
import os.path
import sys
import time

import sqlalchemy as sa

from paste.deploy import appconfig

from dane import model
from dane.config.environment import load_environment
from dane.lib import userstate

# setup the loggers
STDOUT_LEVEL = logging.INFO
log = logging.getLogger()
log.setLevel(STDOUT_LEVEL)
formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s", "%Y-%m-%d %H:%M:%S")
out = logging.StreamHandler(sys.stdout)
out.setFormatter(formatter)
out.setLevel(STDOUT_LEVEL)
log.addHandler(out)

BATCH_SIZE = 5000
REBAL_BATCH_SIZE = 20000

def main(engine):
    log.info("starting run, BATCH_SIZE = %s" % BATCH_SIZE)
    stmt = sa.text("""SELECT user_id FROM user_state""")
    resultset = engine.execute(stmt)
    counter = 0
    rebal_count = 0
    prev_time = time.time()
    bucket_extension_base = "r-%s-" % time.time()
    bucket_extension = "%s%s" % (bucket_extension_base, rebal_count)
    for row in resultset:
        try:
            user_id = row['user_id']
            rebal_count += userstate.rebalance(user_id, bucket_extension)
        except Exception, e:
            log.exception("exception processing %s" % user_id)

        if rebal_count % REBAL_BATCH_SIZE == 0:
            bucket_extension = "%s%s" % (bucket_extension_base, rebal_count)

        counter += 1
        if counter % BATCH_SIZE == 0:
            proc_time = time.time() - prev_time
            log.info("%s processed, time = %s, user_id = %s, rebal = %s" % 
                     (counter, proc_time, user_id, rebal_count))
            prev_time = time.time()

    log.info("DONE, processed = %s, rebal = %s" % (counter, rebal_count))

def print_usage():
    print "usage: python process_userstates_datafiles.py path/to/datadir"
    sys.exit(1)

def configure():
    parser = optparse.OptionParser()
    parser.add_option("-d", "--shard", dest="shard", default="0", help="shard to process")
    parser.add_option("-i", "--ini", dest="ini", default="staging.ini", help="dane ini file")
    options, args = parser.parse_args()

    ini_file = options.ini
    shard = int(options.shard)
    config = appconfig('config:' + os.path.realpath(ini_file))
    load_environment(config.global_conf, config.local_conf, check_swfs=False)

    dane_us_write = config['sqlalchemy.dane_userstate%s.url' % shard]
    dane_us_engine = sa.create_engine(dane_us_write).connect()

    return dane_us_engine

if __name__ == '__main__':
    engine = configure()

    import gc
    log.info("GC collected %r objects", gc.collect())
    
    main(engine)
