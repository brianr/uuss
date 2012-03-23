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
from uuss.scripts.process_userstates_buckets import _app_id_from_user_id, gen_userstate_line, gen_player_line
from lolapps import helpers

# logging
log = processor.configure_logging()

max_batch_size = None
mongo_db = None
tmp_dir = None
filename = None
bucket = None
shard = None
rebalancing = False
filetype = None
game = None
userstate = None

def do_processing():
    load_name = 'user_state-modified-%s-%s' % (bucket, time.time())

    mod_ids = open(filename, 'r').readlines()
    mod_ids = [id.replace('\n', '').strip() for id in mod_ids]
    log.info("%s ids found to process" % len(mod_ids))

    mod_count = 0
    batch_count = 1
    while mod_ids:
        proc_ids = mod_ids[:max_batch_size]
        mod_ids = mod_ids[max_batch_size:]
        base_file_name = "%s.%s" % (load_name, batch_count)
        log.info("Writing %s", base_file_name)
        with open(os.path.join(tmp_dir, base_file_name+".usrtmp"), 'w') as user_file:
            with open(os.path.join(tmp_dir, base_file_name+".plrtmp"), 'w') as player_file:
                mod_count += len(proc_ids)
                for row in proc_ids:
                    try:
                        if filetype == "player":
                            id = row.split(',')[0][2:-1]
                        else:
                            id = row.split("\t")[0][1:-1]
                        state = userstate.backup(id)
                        if isinstance(state, str):
                            state = simplejson.loads(zlib.decompress(state))
                        raw_state = chunking.blow_chunks(state)
                        user_line = gen_userstate_line(id, raw_state)
                        player_line = gen_player_line(id, state)
                        user_file.write(user_line+'\n')
                        if player_line:
                            player_file.write(player_line+'\n')
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
        log.info("processed batch, %s remaining" % len(mod_ids))
        batch_count += 1

    mongo_db.drop_collection(load_name)

    log.info("Processed")


def configure(options):
    """
    Find the first app we got command-line options for and setup for it.
    We are only processing one app with this script, so if more than one
    is specified on the command-line, only one of them will get processed.
    No guarantees on which one that is.
    """
    global mongo_db, tmp_dir, max_batch_size, filename, bucket, shard, rebalancing, filetype, game, userstate

    log.info('Configuring...')

    ini_file = options.ini
    filename = options.file
    game = options.game

    if filename[-8:-4] == "user":
        filetype = "user"
    else:
        filetype = "player"
    
    max_batch_size = int(options.batch)
    bucket = int(options.bucket)
    shard = int(options.shard)

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


if __name__ == '__main__':
    parser = optparse.OptionParser()
    # default to staging.ini so never accidentaly run against production
    parser.add_option("-i", "--ini", dest="ini", default="staging.ini", help="dane ini file")
    parser.add_option("-b", "--batch", dest="batch", default="1000", help="maximum batch size")
    parser.add_option("-f", "--file", dest="file", help="userstate.player.bad file to process")
    parser.add_option("-u", "--bucket", dest="bucket", default="10", help="fake bucket to process")
    parser.add_option("-s", "--shard", dest="shard", default="0", help="shard to process")
    parser.add_option("-r", "--rebal", dest="rebal", action="store_true", default=False, 
                      help="rebalancing node")
    parser.add_option("-g", "--game", dest="game", help="game to process (dane, india)")
    options, args = parser.parse_args()
    configure(options)

    do_processing() # run once
