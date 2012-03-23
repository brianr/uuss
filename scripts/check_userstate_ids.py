"""
"""
import logging
import multiprocessing
import optparse
import os
import os.path
import sys
import time

import simplejson
import sqlalchemy as sa
import zlib

from lolapps.util.adapters import chunking

# setup the loggers
log = None

START_IDX = 0
#START_IDX = 23100000
END_IDX = 3500000
BATCH_SIZE = 5000
NUM_PROCS = multiprocessing.cpu_count() * 2
#NUM_PROCS = 2


class fix_state(object):
    fixed_userstates = {'100000002892909': 1, '100000005257921': 1, '100000009265636': 1, '100000019291850': 1, '100000028214242': 1, '100000030798593': 1, '100000036057821': 1, '100000037613452': 1, '100000042964863': 1, '100000046668439': 1, '100000097592964': 1, '100000105300555': 1, '100000108949079': 1, '100000124159434': 1, '100000129369320': 1, '100000136608482': 1, '100000138741041': 1, '100000143235995': 1, '100000144180973': 1, '100000145328786': 1, '100000165845709': 1, '100000169996027': 1, '100000177195208': 1, '100000190684929': 1, '100000191251154': 1, '100000197143454': 1, '100000204679743': 1, '100000212287410': 1, '100000232203161': 1, '100000260145773': 1, '100000280570051': 1, '100000299029785': 1, '100000321978980': 1, '100000322533746': 1, '100000328983193': 1, '100000348921179': 1, '100000380085378': 1, '100000386989147': 1, '100000388282004': 1, '100000389510616': 1, '100000408176550': 1, '100000409840584': 1, '100000417201137': 1, '100000425856576': 1, '100000433151391': 1, '100000436833110': 1, '100000484957374': 1, '100000485276205': 1, '100000501837424': 1, '100000516515462': 1, '100000559225361': 1, '100000565813876': 1, '100000573531628': 1, '100000577430355': 1, '100000596367976': 1, '100000599659714': 1, '100000612234281': 1, '100000622740883': 1, '100000625992316': 1, '100000629036861': 1, '100000630884580': 1, '100000636160198': 1, '100000642680364': 1, '100000675669342': 1, '100000678556025': 1, '100000683713625': 1, '100000688721303': 1, '100000699737922': 1, '100000701187645': 1, '100000701227186': 1, '100000705432715': 1, '100000706847577': 1, '100000707248027': 1, '100000723373694': 1, '100000733784955': 1, '100000734500071': 1, '100000743897421': 1, '100000824911425': 1, '100000843077753': 1, '100000848746702': 1, '100000848931468': 1, '100000856184853': 1, '100000856844505': 1, '100000870909736': 1, '100000875302709': 1, '100000889721313': 1, '100000895810495': 1, '100000896420388': 1, '100000923145832': 1, '100000923291815': 1, '100000930248313': 1, '100000941576692': 1, '100000941988148': 1, '100000978798952': 1, '100001003152277': 1, '100001029128149': 1, '100001030296444': 1, '100001048273085': 1, '100001051943676': 1, '100001062966991': 1, '100001065957321': 1, '100001067468934': 1, '100001069355754': 1, '100001080836916': 1, '100001084511434': 1, '100001088732580': 1, '100001093653337': 1, '100001109528567': 1, '100001119581783': 1, '100001124786554': 1, '100001126100861': 1, '100001142267142': 1, '100001144363642': 1, '100001145494351': 1, '100001155289795': 1, '100001174891830': 1, '100001204103231': 1, '100001208570590': 1, '100001213713524': 1, '100001218957348': 1, '100001219760916': 1, '100001242654417': 1, '100001246062916': 1, '100001253831270': 1, '100001276045454': 1, '100001294198253': 1, '100001321328178': 1, '100001323547759': 1, '100001367419463': 1, '100001402159280': 1, '100001428158680': 1, '100001446300304': 1, '100001468015143': 1, '100001508889610': 1, '100001545526861': 1, '100001547835585': 1, '100001643771075': 1, '100001659221402': 1, '100001708850871': 1, '100001728857622': 1, '100001730617331': 1, '100001802856434': 1, '100001824768167': 1, '100001856012854': 1, '100001891844359': 1, '100001908135739': 1, '100001911671152': 1, '100001949916050': 1, '100001952902124': 1, '100001993796187': 1, '100002127190859': 1, '1000283987': 1, '1001106793': 1, '1001325196': 1, '1019775085': 1, '1052032033': 1, '1159608617': 1, '1170583432': 1, '1187457742': 1, '1291016364': 1, '1300185290': 1, '1307654922': 1, '1310822303': 1, '1348375620': 1, '1350360017': 1, '1379868731': 1, '1408023116': 1, '1411060911': 1, '1418143893': 1, '1452881767': 1, '1453171309': 1, '1471339018': 1, '1474874234': 1, '1494627923': 1, '1498807524': 1, '1560559711': 1, '1567078101': 1, '1613941661': 1, '1614942442': 1, '1624004787': 1, '1629347390': 1, '1647595002': 1, '1665181991': 1, '1668944178': 1, '1683428013': 1, '1706726585': 1, '1712034543': 1, '1816585695': 1, '1829921486': 1, '1839074053': 1, '206700271': 1, '512208315': 1, '530775437': 1, '536434819': 1, '602762716': 1, '638330891': 1, '672805465': 1, '712628602': 1, '726826327': 1, '754204626': 1, '764628481': 1, '765209730': 1, '773433356': 1, '805231838': 1, 'vstvwww::vz::net:ieuwxauoyt0mtwgrwh-9iw': 1, 'vstvwww::vz::net:l2knplvdtv2cbscrteikyg': 1}

    @classmethod
    def fix_state(cls, uid, userstate):
        #from uuss.proto import UUSSProtocolException
        #from dane.lib import userstate
        #try:
        #    userstate.get(uid)
        #except UUSSProtocolException, e:
        #    if 'wrong user_id in state' in e.args[0]:
        #        print "Wrong user_id in state for %r" % uid
                if uid in cls.fixed_userstates:
                    log.warn("uid %r has been fixed %r times before" % (uid, cls.fixed_userstates[uid]))
                else:
                    cls.fixed_userstates[uid] = 0
                cls.fixed_userstates[uid] += 1
                userstate.save(uid, {'user_id': uid})
        #return userstate.get(uid)

def main():
    engine, model, config = configure()
    shards = int(config['sqlalchemy.dane_userstate.shard_count'])
    log.info("%r shards", shards)
    m = multiprocessing.Manager()
    stats = m.dict({'processed': 0, 'fixed': 0, 'chunks_done': 0})
    ml = multiprocessing.Process(target=main_logger, args=(stats,))
    ml.start()
    procs = []
    for shard in xrange(shards):
        log.info("Starting for shard %r", shard)
        proc = multiprocessing.Process(target=main_shard, args=(shard, stats))
        proc.start()
        procs.append(proc)

    for proc in procs:
        proc.join()

def main_logger(stats):
    configure()
    while True:
        log.info("*** Overall stats %r", dict([t for t in stats.items()]))
        time.sleep(5)

def main_shard(shard, stats):
    #i = 14074449

#    engine, model, config = configure(shard)
#    log.info("Getting count for shard %r", shard)
#    stmt = sa.text("""SELECT COUNT(*) FROM user_state""")
#    res = engine.execute(stmt)
#    END_IDX = res.fetchone()[0]
    log.info("START_IDX: %i, END_IDX: %i", START_IDX, END_IDX)

    counter = 0
    not_in_memcache = 0

    m = multiprocessing.Manager()
    state = m.dict()
    state['end_found'] = False
    state['end_idx'] = 0

    pool = multiprocessing.Pool(NUM_PROCS)
    pool.map(run_batch, [(stats, i * BATCH_SIZE, shard, state) for i in xrange(START_IDX / BATCH_SIZE, END_IDX / BATCH_SIZE)], 1)
    log.info("DONE, i = %r, processed = %r, not_in_memcache = %r" % (i, counter, not_in_memcache))
    ml.terminate()

def run_batch(args):
    stats, i, shard, state = args

    if state['end_found'] and i > state['end_idx']:
        log.debug("Past the end, skipping %r", i)
        return

    engine, model, config = configure(shard)

    import gc
    log.info("GC collected %r objects", gc.collect())
    
    counter = 0
    fixed = 0

    log.info("starting run, i = %r, BATCH_SIZE = %r" % (i, BATCH_SIZE))
    stmt = sa.text("""SELECT user_id FROM user_state LIMIT %r OFFSET %r""" % (BATCH_SIZE, i))
    resultset = engine.execute(stmt)
    prev_time = time.time()

    from uuss import WrongUserIdException
    go = False
    for row in resultset:
        go = True
        i += 1
        try:
            user_id = row['user_id']
            try:
                model.userstate.get(user_id)
            except WrongUserIdException, e:
                #user_state = model.UserState.get(user_id)
                #try:
                #    state = chunking.reconstitute_chunks(user_state.state, True)
                #except:
                #    chunking_exc = sys.exc_info()
                #    try:
                #        state = simplejson.loads(zlib.decompress(user_state.state))
                #    except:
                #        # If we get here then both chunking and non-chunking failed, so let's raise the original exc
                #        log.exception("Both chunking and non-chunking parsing failed, below is the non-chunking exception, the chunking exception has been raised")
                #        raise chunking_exc[0], chunking_exc[1], chunking_exc[2]
                #try:
                #    uuss.check_user_id(user_id, state)
                #except:
                #    bad_in_db += 1
                #else:
                #    good_in_db += 1

                fix_state.fix_state(user_id, model.userstate)
                stats['fixed'] += 1
                fixed += 1
        except:
            log.exception("exception processing %s" % user_id)

        counter += 1
        if i % 100 == 0:
            log.info("i = %r, processed = %r, user_id = %r, fixed = %r" % 
                     (i, counter, user_id, fixed))
        if counter % 20 == 0:
            stats['processed'] += 20

#        proc_time = time.time() - prev_time
#        log.info("i = %r, processed = %r, time = %r, user_id = %r, not_in_memcache = %r" % 
#                 (i, counter, proc_time, user_id, not_in_memcache))
#        prev_time = time.time()
    stats['processed'] += counter % 20
#    stats['fixed'] += fixed
    stats['chunks_done'] += 1
    if not go or counter != BATCH_SIZE:
        state['end_found'] = True
        state['end_idx'] = i

def configure(shard=None):
    import argparse

    parser = argparse.ArgumentParser(description='Run through all user ids in the state tables and check the user_ids in the states')

    parser.add_argument("-i", "--ini", help="ini file", required=True)
    parser.add_argument("-g", "--game", help="game", required=True)
    args = parser.parse_args()

    from uuss.server import configuration
    (full_config, config) = configuration.init_from_ini(args.ini)

    global log
    log = logging.getLogger(__name__)

    from uuss.server import model

    game_model = getattr(model, args.game)

    if shard is not None:
#        engine = sa.engine_from_config(config, "sqlalchemy.%s_read." % args.game)
        engine = game_model.userstate_engines['userstate_shard_%i' % shard]
    else:
        engine = None
    return engine, game_model, config

if __name__ == '__main__':
    main()
