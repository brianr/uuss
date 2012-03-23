from uuss.server.model import base
from lolapps.pylons.model.cache import parse_memcache_config, NamespacedMemcacheClient
from lolapps.pylons.model.mq import NamespacedServer as NamespacedMQServer
from lolapps.pylons.model.orm import Schema
from lolapps.util.adapters import hash_ring
from lolapps.util.adapters.raidcache import RaidcacheClient
from paste.deploy import converters
from sqlalchemy import orm
import sqlalchemy as sa


MQ_UUSS = 'uuss'

mq = None

def init(games, config, engines, write_engines):
    global mq
    
    mq_config = parse_memcache_config(config['mq.server'])
    if len(mq_config) != 1:
        raise Exception("Only 1 mq server allowed")
    mq = NamespacedMQServer(mq_config[0][0], config['mq.namespace'])

    if converters.asbool(config.get('uuss.use_gevent', True)):
        # Use the gevent socket pool for raidcache and the non-thread-local raidcache client
        from lolapps.common.gevent.socket_pool import SocketPool
    else:
        from lolapps.util.socket_pool import NoopPool as SocketPool

    import sys
    this_module = sys.modules[__name__]

    from uuss import client
    for game in converters.aslist(config.get('uums.remote_games', None), ','):
        if not game:
            continue
        a_model = base.BaseModel()
        setattr(this_module, game, a_model)
        a_model.userstate = client.UserState(
            {
                'app_id': 'uuss-remote-%s' % game,
                'user_state.uuss_server': config['uums.%s_uuss_server' % game] },
            game)
        a_model.userstate.send_message = a_model.userstate.send_message_from
    
    # configure userstate models
    from lolapps.common import userstate_multi
    for game in games:
        mysql_write = converters.asbool(config['%s_user_state.mysql_write' % game])
        a_model = base.BaseModel()
        setattr(this_module, game, a_model)
        a_model.game = game
        a_model.userstate_engines = engines['%s_userstate_engines' % game]
        a_model.userstate_ring = hash_ring.HashRing(a_model.userstate_engines)

        a_model.write_userstate_engines = write_engines['%s_userstate_engines' % game]
        a_model.write_userstate_ring = hash_ring.HashRing(a_model.write_userstate_engines)

        a_model.UserstateSessions = {}
        a_model.WriteUserstateSessions = {}
        for key in a_model.userstate_engines:
            a_model.UserstateSessions[key] = orm.scoped_session(
                # if we're not writing to mysql, we don't need to be transactional since we're only reading
                orm.sessionmaker(
                    transactional=mysql_write,
                    autoflush=False,
                    bind=a_model.userstate_engines[key])
            )
            if mysql_write:
                a_model.WriteUserstateSessions[key] = a_model.UserstateSessions[key]
            else:
                a_model.WriteUserstateSessions[key] = orm.scoped_session(
                    orm.sessionmaker(
                        transactional=True,
                        autoflush=False,
                        bind=a_model.write_userstate_engines[key])
                )

        a_model.lock_mc = NamespacedMemcacheClient(config['%s_lock_memcache.namespace' % game],
                                                   parse_memcache_config(config['%s_lock_memcache.server_list' % game]))
        
        a_model.userstate = userstate_multi.UserState(
            config,
            a_model,
            game=game,
            update_mysql_date_modified=True,
            raidcache_client_class=RaidcacheClient,
            raidcache_pool_class=SocketPool)
        a_model.metadata = sa.MetaData()
        a_model.schema = Schema()
    
        a_model.schema.autoload('user_state', a_model.metadata, a_model.userstate_engines.values()[0])
        a_model.UserstateSessions.values()[0].mapper(
            a_model.UserState,
            a_model.schema.user_state,
            properties={'state': orm.synonym('_ustate', map_column=True)})
