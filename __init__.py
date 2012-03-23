from lolapps.util.adapters import chunking

import logging

log = logging.getLogger(__name__)

## game names for use with the UUMS and UUSS
DANE='dane'
INDIA='india'
RICH='rich'
VICTORIA='victoria'

ALL_GAMES = [ DANE, INDIA, RICH, VICTORIA ]


class UserstateException(Exception):
    pass

class UserIdMissingException(UserstateException):
    pass

class WrongUserIdException(UserstateException):
    pass

def check_user_id(user_id, state, chunk_config={}, game=None):
    if state is None:
        return
    # if we get a str, this is a chunked state and needs to have the master record reconstitued so we can check the user_id
    if isinstance(state, str):
        if chunk_config == {}:
            config = chunking.get_saved_chunk_config(state)
            if isinstance(config, list):
                # we can't check for the user id and game in the old format list chunk config
                state = chunking.reconstitute_chunks(state, True)
            else:
                # If no chunk config passed in to check, just check the values in the chunk config
                state = config
        else:
            # reconstitute so we can check the chunks
            state = chunking.reconstitute_chunks(state, True)
    if game is not None:
        if 'game' not in state:
            log.debug("game not set in state, adding")
            state['game'] = game
        elif state['game'] != game:
            raise UserstateException("state['game'] (%r) != game (%r)" % (state['game'], game))
    if 'user_id' not in state:
        raise UserIdMissingException("user_id missing for '%s'" % user_id)
    elif state['user_id'] != str(user_id):
        raise WrongUserIdException("wrong user_id in state for '%s', found '%s'" % (user_id, state['user_id']))

    for chunk_name, chunk_path in chunk_config.iteritems():
        container = state
        for key in chunk_path[:-1]:
            if key not in container:
                container = None
                break
            container = container[key]
            
        if container is None:
            continue

        chunk_key = chunk_path[-1]

        if chunk_key in container and container[chunk_key] is None:
            raise UserstateException("Chunk %r is None for user %r" % (chunk_name, user_id))
