import numpy as np
from .functions import functions as fun

def assert_same_result(r1, r2):
    if isinstance(r1, np.ndarray):
        np.testing.assert_array_equal(r1, r2)
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
