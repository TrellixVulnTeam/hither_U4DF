import numpy as np
import pytest
from .functions import functions as fun

def assert_same_result(r1, r2):
    assert type(r1) == type(r2)
    if isinstance(r1, np.ndarray):
        np.testing.assert_array_equal(r1, r2)
    elif type(r1) == list:
        assert len(r1) == len(r2)
        for i in range(len(r1)):
            assert_same_result(r1[i], r2[i])
    elif type(r1) == tuple:
        assert len(r1) == len(r2)
        for i in range(len(r1)):
            assert_same_result(r1[i], r2[i])
    elif type(r1) == dict:
        for k in r1.keys():
            assert k in r2
            assert_same_result(r1[k], r2[k])
        for k in r2.keys():
            assert k in r1
    else:
        assert r1 == r2

def assert_same_exception(e1, e2):
    return str(e1) == str(e2)

def test_call_functions_directly(general):
    functions = [getattr(fun, k) for k in dir(fun)]
    for function in functions:
        if callable(function) and hasattr(function, 'test_calls'):
            for test_call in function.test_calls:
                if not test_call.get('container_only'):
                    args = test_call.get('args')
                    print(f'Calling {function.__name__} {args}')
                    try:
                        result = function(**args)
                        print(result, type(result))
                        if 'result' in test_call:
                            assert_same_result(result, test_call['result'])
                    except Exception as e:
                        if 'result' in test_call:
                            raise
                        elif 'exception' in test_call:
                            assert_same_exception(e, test_call['exception'])

def test_run_functions(general):
    functions = [getattr(fun, k) for k in dir(fun)]
    for function in functions:
        if callable(function) and hasattr(function, 'test_calls'):
            for test_call in function.test_calls:
                if not test_call.get('container_only'):
                    args = test_call.get('args')
                    print(f'Running {function.__name__} {args}')
                    try:
                        result = function.run(**args).wait()
                        print(result, type(result))
                        if 'result' in test_call:
                            assert_same_result(result, test_call['result'])
                    except Exception as e:
                        if 'result' in test_call:
                            raise
                        elif 'exception' in test_call:
                            assert_same_exception(e, test_call['exception'])
