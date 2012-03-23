import base64
import simplejson
import time
import types
import zlib

import logging
from lolapps.util.adapters import chunking

log = logging.getLogger(__name__)


def make_empty_user_state(user_id, game=None):
    """
    Return a python object representing an empty user state
    """
    state = {'user_id': str(user_id)}
    if game:
        state['game'] = str(game)
    return state


class UserstateORMBase(object):
    """
    Base class for ORM classes that use the userstate session.
    """
    
    @classmethod
    def get(cls, real_cls, user_id, UserstateSessions, userstate_ring):
        """
        real_cls is the subclass which should be queried.
        user_id is the user id
        UserstateSessions should be the model.UserstateSessions for the project
        userstate_ring should be the model.userstate_ring for the project
        """
        return UserstateSessions[userstate_ring.get_node(str(user_id))].query(real_cls).get([user_id])
    
    @classmethod
    def get_cluster(cls, user_id, userstate_ring):
        return userstate_ring.get_node(str(user_id))

    @classmethod
    def delete(cls, real_cls, user_id, UserstateSessions, userstate_ring):
        """
        real_cls is the subclass which should be queried.
        user_id is the user id
        UserstateSessions should be the model.UserstateSessions for the project
        userstate_ring should be the model.userstate_ring for the project
        """
        session = UserstateSessions[userstate_ring.get_node(str(user_id))]
        state = session.query(real_cls).get([user_id])
        if state:
            session.delete(state)
            session.commit()
    
    @classmethod
    def find(cls, **kw):
        pass
    
    @classmethod
    def _find(cls, **kw):
        pass
    
    @classmethod
    def find_all(cls, **kw):
        pass
    
    def before_update(self):
        pass
    
    def after_update(self):
        pass
    
    def before_delete(self):
        pass
    
    def after_delete(self):
        pass
    
    def after_insert(self):
        pass
    
class UserState(UserstateORMBase):
    """
    Subclasses need to reimplement get and get_cluster to pass in the
    UserstateSessions and userstate_ring as appropriate. get also
    needs the subclass passed into it.

    ex:
    from project_name import model
    
    class UserState(UserstateORMBase):
        @classmethod
        def get(cls, user_id):
            return UserState.get(cls, user_id, model.UserstateSessions, model.userstate_ring)
            
        @classmethod
        def get_cluster(cls, user_id):
            return UserState.get_cluster(user_id, model.userstate_ring)
            
        @classmethod
        def create(cls, user_id):
            return UserState.get(cls, user_id, model.game)
    """
    def __init__(self, *a, **kw):
        super(UserState, self).__init__(*a, **kw)
    
    @classmethod
    def create(cls, orig_cls, user_id, game):
        user_state = orig_cls()
        user_state.user_id = user_id
        user_state.state = chunking.blow_chunks(make_empty_user_state(user_id, game))
        user_state.date_created = int(time.time())
        user_state.date_modified = int(time.time())
        return user_state
    
    def _setstate(self, state):
        """
        Stores a zlib-compressed and base64 encoded version of user state                  
        """ 
        log.debug("setting state for %r", self.user_id)
        self._ustate_raw = state
        self._ustate = base64.b64encode(state)
                                                                                          
    def _getstate(self):                                                                  
        """
        Gets a b64decoded/zlib-decompressed version of user state
        """
        if not hasattr(self, "_ustate_raw"):
            try:
                self._ustate_raw = base64.b64decode(self._ustate)
            except TypeError, e:
                log.debug("Couldn't not b64decode state for user_id:%r = %r. Error %r", 
                    self.user_id, self._ustate, e)
                self._ustate_raw = self._ustate
            
        return self._ustate_raw
    
    state = property(_getstate, _setstate)


class BaseModel(object):
    """
    Emulates the model object found in individual projects.
    """
    def __init__(self):
        self.game = None
        
        self.userstate_ring = None
        self.userstate_engines = None

        self.write_userstate_ring = None
        self.write_userstate_engines = None

        self.userstate = None
        self.lock_mc = None

        self.UserState = None
        self.UserstateSessions = None
        self.WriteUserstateSessions = None

        self.metadata = None
        self.schema = None

        model_obj = self

        class SessionBoundUserState(UserState):
            """
            Subclass of UserState above which has get(), delete(), and get_cluster() overridden
            to pass in UserstateSessions and userstate_ring from this object's instance.
            """
            def __init__(self, *a, **kw):
                super(SessionBoundUserState, self).__init__(*a, **kw)
            
            @classmethod
            def create(cls, user_id):
                return UserState.create(cls, user_id, model_obj.game)
        
            @classmethod
            def get(cls, user_id):
                return UserState.get(cls, user_id, self.UserstateSessions, self.userstate_ring)

            @classmethod
            def delete(cls, user_id):
                return UserState.delete(cls, user_id, self.WriteUserstateSessions, self.write_userstate_ring)

            @classmethod
            def get_cluster(cls, user_id):
                return UserState.get_cluster(user_id, self.userstate_ring)

        self.UserState = SessionBoundUserState
        self.make_empty_user_state = make_empty_user_state

