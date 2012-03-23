import ConfigParser
import logging

from lolapps.pylons.model import orm

from paste.deploy import converters
import sqlalchemy as sa
import sqlalchemy.interfaces as sai
import _mysql_exceptions

from uuss.server import model

log = logging.getLogger(__name__)


def init_from_ini(ini_file, games=None, overrides=None):
    import logging
    import logging.config
    
    logging.config.fileConfig(ini_file)

    # Reset log after logging has been configured
    global log
    log = logging.getLogger(__name__)

    full_config = ConfigParser.SafeConfigParser()
    full_config.readfp(open(ini_file))
    config = dict(full_config.items('DEFAULT'))

    if overrides:
        config.update(overrides)

    init(config, games)
    return (full_config, config)


def init(config, games=None):
    if converters.asbool(config.get('pdb_debugger', False)):
        import lolapps.util.pdb_debugger
        lolapps.util.pdb_debugger.init()

    orm.init_mysql_connection_listeners(config)

    engines = {}
    all_engines = {}
    write_engines = {}

    if games is None:
        games = converters.aslist(config['uuss_games'], ',')

    # Grab the userstate engines and connection strings.
    for game in games:
        mysql_write = converters.asbool(config['%s_user_state.mysql_write' % game])
        engine_key, string_key = '%s_userstate_engines' % game, '%s_userstate_strings' % game
        engines[engine_key] = {}
        engines[string_key] = []
        write_engines[engine_key] = {}
        write_engines[string_key] = []
        for i in xrange(int(config['sqlalchemy.%s_userstate.shard_count' % game])):
            conn_string = "userstate_shard_%s" % i
            sqlalchemy_key = "%s_userstate_read%s" % (game, i)
            engine = sa.engine_from_config(
                config, "sqlalchemy.%s." % (sqlalchemy_key)
            )
            engines[engine_key][conn_string] = engine
            engines[string_key].append(conn_string)
            all_engines[sqlalchemy_key] = engine
            sqlalchemy_key = "%s_userstate%s" % (game, i)
            if mysql_write:
                write_engines[engine_key][conn_string] = engine
                all_engines[sqlalchemy_key] = engine
            else:
                write_engine = sa.engine_from_config(
                    config, "sqlalchemy.%s." % (sqlalchemy_key)
                )
                write_engines[engine_key][conn_string] = write_engine
                all_engines[sqlalchemy_key] = write_engine

    orm.update_mysql_connection_listeners(config, all_engines)

    model.init(games, config, engines, write_engines)
