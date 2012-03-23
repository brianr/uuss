import copy
import logging
from lolapps.common import uums
from nose.tools import raises
import random


log = logging.getLogger(__name__)


class BasicTests(object):
    def __init__(self):
        self._state = None
        self.test_message = {'msg': 'test message'}

    @property
    def state(self):
        return copy.deepcopy(self._state)
        
    def test_add_get(self):
        state = self.state
        prenum = len(uums.get_messages_from_state(state))
        msg_id = uums.add_message_to_state('india', 'source_test_user', state, self.test_message)
        msgs = uums.get_messages_from_state(state)
        assert len(msgs) == prenum + 1
        assert msgs[-1]['id'] == msg_id

    def test_add_remove(self):
        state = self.state
        msg_id = uums.add_message_to_state('india', 'source_test_user', state, self.test_message)
        prenum = len(uums.get_messages_from_state(state))
        uums.remove_messages_from_state(state, [ msg_id ])
        msgs = uums.get_messages_from_state(state)
        assert len(msgs) == prenum - 1

    def test_multi_add_remove(self):
        num = 50
        state = self.state
        msg_ids = []
        prenum = len(uums.get_messages_from_state(state))
        for i in range(num):
            msg_ids.append(uums.add_message_to_state('india', 'source_test_user_%i' % i, state, "%s_%i" % (self.test_message, i)))
        msgs = uums.get_messages_from_state(state)
        assert len(msgs) == prenum + num
        prenum = len(msgs)
        uums.remove_messages_from_state(state, msg_ids)
        msgs = uums.get_messages_from_state(state)
        assert len(msgs) == prenum - num
        for msg in msgs:
            assert msg['id'] not in msg_ids

    def test_multi_add_shuffled_remove(self):
        for i in range(10):
            num = 50
            state = self.state
            msg_ids = []
            prenum = len(uums.get_messages_from_state(state))
            for i in range(num):
                msg_ids.append(uums.add_message_to_state('india', 'source_test_user_%i' % i, state, "%s_%i" % (self.test_message, i)))
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum + num
            prenum = len(msgs)
            random.shuffle(msg_ids)
            uums.remove_messages_from_state(state, msg_ids)
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum - num
            for msg in msgs:
                assert msg['id'] not in msg_ids

    def test_multi_add_shuffled_one_by_one_remove(self):
        for i in range(10):
            num = 50
            state = self.state
            msg_ids = []
            prenum = len(uums.get_messages_from_state(state))
            for i in range(num):
                msg_ids.append(uums.add_message_to_state('india', 'source_test_user_%i' % i, state, "%s_%i" % (self.test_message, i)))
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum + num
            prenum = len(msgs)
            random.shuffle(msg_ids)
            for msg_id in msg_ids:
                uums.remove_messages_from_state(state, [msg_id])
                msgs = uums.get_messages_from_state(state)
                for msg in msgs:
                    assert msg['id'] != msg_id
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum - num
            for msg in msgs:
                assert msg['id'] not in msg_ids

    def test_random_multi_add_remove(self):
        for i in range(10):
            num = random.randint(50, 100)
            state = self.state
            msg_ids = []
            prenum = len(uums.get_messages_from_state(state))
            for i in range(num):
                msg_ids.append(uums.add_message_to_state('india', 'source_test_user_%i' % i, state, "%s_%i" % (self.test_message, i)))
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum + num
            prenum = len(msgs)
            sample_num = random.randint(10, num - 10)
            to_remove = random.sample(msg_ids, sample_num)
            uums.remove_messages_from_state(state, to_remove)
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum - len(to_remove), "%i != %i - %i" % (len(msgs), prenum, len(to_remove))
            for msg in msgs:
                assert msg['id'] not in to_remove

    def test_random_multi_add_one_by_one_remove(self):
        for i in range(10):
            num = random.randint(50, 100)
            state = self.state
            msg_ids = []
            prenum = len(uums.get_messages_from_state(state))
            for i in range(num):
                msg_ids.append(uums.add_message_to_state('india', 'source_test_user_%i' % i, state, "%s_%i" % (self.test_message, i)))
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum + num
            prenum = len(msgs)
            to_remove = random.sample(msg_ids, random.randint(10, num - 10))
            for msg_id in to_remove:
                uums.remove_messages_from_state(state, [msg_id])
                msgs = uums.get_messages_from_state(state)
                for msg in msgs:
                    assert msg['id'] != msg_id
            msgs = uums.get_messages_from_state(state)
            assert len(msgs) == prenum - len(to_remove)
            for msg in msgs:
                assert msg['id'] not in to_remove

    @raises(uums.UUMSException)
    def test_remove_unknown_message(self):
        state = self.state
        uums.remove_messages_from_state(state, ['unknown'])
        

class TestEmptyState(BasicTests):
    def setup(self):
        self._state = {'user_id': 'test_user'}

    def test_get_messages_from_empty_state(self):
        state = self.state
        assert 'uums' not in state
        assert uums.get_messages_from_state(state) == []
        assert 'uums' in state
        assert 'messages' in state['uums']
        assert state['uums']['messages'] == []

    def test_add_message_to_empty_state(self):
        state = self.state
        assert 'uums' not in state
        msg_id = uums.add_message_to_state('india', 'source_test_user', state, self.test_message)
        assert 'uums' in state
        assert 'messages' in state['uums']
        assert msg_id is not None
        assert len(state['uums']['messages']) == 1
        assert state['uums']['messages'][0]['message'] == self.test_message
        assert state['uums']['messages'][0]['source_game'] == 'india'
        assert state['uums']['messages'][0]['source_user_id'] == 'source_test_user'

    @raises(uums.UUMSException)
    def test_remove_messages_from_empty_state_raises_exception(self):
        state = self.state
        assert 'uums' not in state
        uums.remove_messages_from_state(state, ['unknown'])


class TestStateWithNoMessages(BasicTests):
    def setup(self):
        self._state = {'user_id': 'test_user'}
        uums.get_messages_from_state(self.state)


class TestStateWithMessages(BasicTests):
    def setup(self):
        self._state = {'user_id': 'test_user'}
        uums.add_message_to_state('dane', 'source_test_user', self.state, {'key': 'value'})
