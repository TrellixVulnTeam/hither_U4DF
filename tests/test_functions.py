import numpy as np
import pytest
import hither2 as hi
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
            for test_call in function.test_calls():
                if not test_call.get('container_only'):
                    args = test_call.get('args')
                    # the following is needed for the case where we send in a hi.File object
                    args = hi._resolve_files_in_item(args)
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

def do_test_run_functions(container=False):
    functions = [getattr(fun, k) for k in dir(fun)]
    tasks = []
    for function in functions:
        if callable(function) and hasattr(function, 'test_calls'):
            for test_call in function.test_calls():
                do_run = True
                if test_call.get('container_only'):
                    if not container:
                        do_run = False
                if do_run:
                    args = test_call.get('args')
                    print(f'Running {function.__name__} {args}')
                    job = function.run(**args)
                    tasks.append(dict(
                        job=job,
                        test_call=test_call
                    ))
    for task in tasks:
        test_call = task['test_call']
        job = task['job']
        try:
            result = job.wait()
            print(result, type(result))
            if 'result' in test_call:
                assert_same_result(result, test_call['result'])
        except Exception as e:
            if 'result' in test_call:
                raise
            elif 'exception' in test_call:
                if test_call['exception'] is True:
                    pass
                else:
                    assert_same_exception(e, test_call['exception'])

def test_run_functions(general):
    with hi.config(container=False):
        do_test_run_functions()

@pytest.mark.container
def test_run_functions_in_container(general):
    with hi.config(container=True, job_handler=hi.ParallelJobHandler(num_workers=20)):
        do_test_run_functions()
