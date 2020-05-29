import os
import pytest
import hither as hi

@pytest.mark.current
def test_misc():
    with pytest.raises(hi.DuplicateFunctionException):
        from .doubly_defined import doubly_defined_1, doubly_defined_2

def test_event_stream_client_env_password():
    os.environ['TESTENV'] = 'test_password'
    esc = hi.EventStreamClient(url='dummy', channel='test', password=dict(env='TESTENV'))
    assert esc._resolve_password() == 'test_password'