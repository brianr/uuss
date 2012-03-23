from __future__ import with_statement

import base64
import optparse
import os
import pymongo
import simplejson
import sys
import time
import zlib

from paste.deploy import converters

from lolapps.util import processor
from lolapps.util import db
from lolapps.util.adapters import mongo, chunking

from lolapps.common import userstate_multi
from uuss.server import model
from lolapps import helpers

# logging
log = processor.configure_logging()

game = None
max_batch_size = None
mongo_db = None
tmp_dir = None
bucket = None
shard = None
cycle_time = None
rebalancing = False
userstate = None


def _app_id_from_user_id(user_id):
    return helpers.app_id_from_user_id(user_id)


def do_processing(onerun=False):
    for collection_name in mongo_db.collection_names():
        proc_time = 0
        if collection_name.startswith('user_state-modified-%s' % bucket):
            try:
                load_name = 'user_state-modified-%s-%s' % (bucket, time.time())
                try:
                    mongo_db[collection_name].rename(load_name)
                except pymongo.errors.OperationFailure:
                    # effectively this makes sure the renamed collection exists
                    if mongo_db[load_name].count() <= 0:
                        raise
                    log.error("Error encountered renaming collection %s to %s, but able to continue" %
                              (collection_name, load_name))
                
                modified = mongo_db[load_name]
                
                start_time = time.time()
                log.info("%s userstates to save for %s ..." % (modified.count(), load_name))

                # prepare for userstate size monitoring
                userstates_processed = 0
                max_userstate_size = -1
                total_userstate_size = 0
                
                mod_count = 0
                mod_rows = list(modified.find())
                batch_count = 1
                while mod_rows:
                    proc_rows = mod_rows[:max_batch_size]
                    mod_rows = mod_rows[max_batch_size:]
                    base_file_name = "%s.%s" % (load_name, batch_count)
                    with open(os.path.join(tmp_dir, base_file_name+".usrtmp"), 'w') as user_file:
                        with open(os.path.join(tmp_dir, base_file_name+".plrtmp"), 'w') as player_file:
                            for row in proc_rows:
                                mod_count += row['count']
                                try:
                                    if row['_id'] == 'null':
                                        log.error("null user_id encountered")
                                    else:
                                        if rebalancing:
                                            (state, chunked) = userstate.get(row['_id'], raw=True)
                                        else:
                                            (state, chunked) = userstate.backup(row['_id'], raw=True)
                                        if state is not None:
                                            if not chunked:
                                                # NOTE(jpatrin): Shouldn't happen, but just in case...
                                                if isinstance(state, str):
                                                    state = simplejson.loads(zlib.decompress(state))
                                                # NOTE(jpatrin): This will be just a single chunk for now,
                                                # but will serve until the state gets saved by the game
                                                raw_state = chunking.blow_chunks(state)
                                            else:
                                                raw_state = state
                                                state = chunking.reconstitute_chunks(raw_state, True)
                                            #state_zlib_json = zlib.compress(simplejson.dumps(state))
                                            #user_line = gen_userstate_line(row['_id'], state_zlib_json)
                                            user_line = gen_userstate_line(row['_id'], raw_state)
                                            player_line = gen_player_line(row['_id'], state)
                                            user_file.write(user_line+'\n')
                                            if player_line:
                                                player_file.write(player_line+'\n')
                                            
                                            # keep userstate size for average and max size tracking
                                            userstates_processed += 1
                                            userstate_size = len(raw_state) / 1024
                                            total_userstate_size += userstate_size
                                            max_userstate_size = max(userstate_size, max_userstate_size)
                                except userstate_multi.UserstateException, e:
                                    log.exception(e)
                                except Exception, e:
                                    # (jay) errors are bad here, but we don't want to keep the 
                                    # rest of the userstates from being saved, so log it and go on
                                    log.exception(e)
                    # don't want the file reader to get to these before we're done, so keep
                    # as temporary name until finished writing
                    os.rename(os.path.join(tmp_dir, base_file_name+".usrtmp"),
                              os.path.join(tmp_dir, base_file_name+".user"))
                    os.rename(os.path.join(tmp_dir, base_file_name+".plrtmp"),
                              os.path.join(tmp_dir, base_file_name+".player"))
                    log.info("processed batch, %s remaining" % len(mod_rows))
                    batch_count += 1

                mongo_db.drop_collection(load_name)

                log.info("Saved.  Total updates since last backup: %s" % mod_count)
                proc_time = time.time() - start_time
                log.info("%s seconds to process userstates" % proc_time)
                
                # if we processed userstates, log their size characteristics
                if userstates_processed:
                    avg_userstate_size = total_userstate_size / userstates_processed
                    log.info("processed userstate sizes(k): avg %r max %r", 
                             avg_userstate_size, max_userstate_size)
            except Exception, e:
                # problems here are bad, but shouldn't stop processing of
                # other collections
                log.exception(e)
        if onerun and proc_time > 0:
            log.debug("one run finished, returning")
            return


def gen_userstate_line(user_id, state):
    state = base64.b64encode(state)

    return "%s\t%s\t%s\t%s\t%s" % (
            db.str_nullsafe_quote(user_id),
            db.str_nullsafe_quote(state),
            db.str_nullsafe_quote(int(time.time())), # date_created
            db.str_nullsafe_quote(int(time.time())), # date_modified
            db.str_nullsafe_quote(1) # revision_id
        )

def gen_player_line(user_id, state):
    player = state.get('player')
    if not player:
        return ''
        #player = {}

    try:
        now = int(time.time())
        last_login = player.get('lastLogin', 0)
        line = "('%s', %s, %s, %s, %s, %s, %s)" % (user_id, 
                                                   _app_id_from_user_id(user_id),
                                                   player['tutorialStep'],
                                                   player['level'],
                                                   player['xp'],
                                                   int(last_login / 86400.0), # last_visit
                                                   now # timestamp
                                                  )
    except KeyError, e:
        line = None # No player data
    return line


def configure(options):
    """
    Find the first app we got command-line options for and setup for it.
    We are only processing one app with this script, so if more than one
    is specified on the command-line, only one of them will get processed.
    No guarantees on which one that is.
    """
    global mongo_db, tmp_dir, max_batch_size, bucket, shard, cycle_time, rebalancing, game, userstate

    game = options.game
    if game is None:
        print "game option is required"
        sys.exit(1)

    ini_file = options.ini
    if ini_file is None:
        print "ini option is required"
        sys.exit(1)

    log.info('Configuring...')
        
    bucket = int(options.bucket)
    shard = int(options.shard)
    max_batch_size = int(options.batch)
    cycle_time = int(options.time)

    from uuss.server import configuration
    (full_config, config) = configuration.init_from_ini(ini_file, [game], {'uuss.use_gevent': False})
    helpers.config = config

    userstate = getattr(model, game).userstate

    tmp_dir = config["%s_user_state.tmp_dir%s" % (game, shard)]
    if not os.path.exists(tmp_dir):
        os.mkdir(tmp_dir)

    rebalancing = converters.asbool(config['%s_user_state.rebalancing' % game]) and options.rebal
    if rebalancing:
        hosts = config['%s_user_state.mongo_hosts_rebal' % game].split(";")
    else:
        hosts = config['%s_user_state.mongo_hosts' % game].split(";")
    mongo_db = mongo.connect(hosts[shard])[config['%s_user_state.mongo_dbname' % game]]


def main():
    import gc
    log.info("GC collected %r objects", gc.collect())
    
    processor.register_signal_handlers()
    processor.start_log_processor(do_processing, cycle_time)


if __name__ == '__main__':
    parser = optparse.OptionParser()
    # default to staging.ini so never accidentaly run against production
    parser.add_option("-i", "--ini", dest="ini", help="uuss ini file")
    parser.add_option("-b", "--batch", dest="batch", default="1000", help="maximum batch size")
    parser.add_option("-u", "--bucket", dest="bucket", default="0", help="bucket to process")
    parser.add_option("-s", "--shard", dest="shard", default="0", help="shard to process")
    parser.add_option("-t", "--time", dest="time", default="120", help="minimum cycle time in seconds")
    parser.add_option("-r", "--rebal", dest="rebal", action="store_true", default=False, 
                      help="rebalancing node")
    parser.add_option("-g", "--game", dest="game", help="game to process (dane, india)")
    options, args = parser.parse_args()
    configure(options)

    main() # run forever
    #do_processing() # run once
