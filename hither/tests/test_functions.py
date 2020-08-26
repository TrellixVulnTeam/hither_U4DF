import os
import numpy as np
import pytest
import hither as hi
from .functions import functions as fun

def assert_same_result(r1, r2):
    if isinstance(r1, np.integer):
        r1 = int(r1)
    elif isinstance(r2, np.integer):
        r2 = int(r2)
    if isinstance(r1, np.floating):
        r1 = float(r1)
    elif isinstance(r2, np.floating):
        r2 = float(r2)

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
                # if function.__name__ != 'additional_file':
                #     do_run = False
                if do_run:
                    args = test_call.get('args')
                    print(f'Running {function.__name__} {args}')
                    job = function.run(**args).set_label(f'Job: {function.__name__}')
                    assert job.get_label() == f'Job: {function.__name__}' # for coverage
                    assert job.get_function_name() == function.__name__# for coverage
                    assert isinstance(job.get_function_version(), str) # for coverage
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
                assert job.get_status() == hi.JobStatus.FINISHED
                job.print_console_out()
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
    with hi.Config(container=False):
        do_test_run_functions()

def test_run_function_by_name(general):
    x = hi.run('add', x=1, y=2).wait()
    assert x is 3

def test_run_functions_with_job_cache(general, tmp_path):
    job_cache_path = str(tmp_path) + '/job-cache'
    os.mkdir(job_cache_path)
    jc = hi.JobCache(path=job_cache_path)
    # jh = hi.ParallelJobHandler(num_workers=4)
    jh = hi.DefaultJobHandler()
    with hi.Config(job_cache=jc, job_handler=jh):
        do_test_run_functions()
        hi.wait()
    num_jobs = jh._internal_counts.num_jobs
    num_submitted_jobs = jh._internal_counts.num_submitted_jobs
    num_errored_jobs = jh._internal_counts.num_errored_jobs
    num_finished_jobs = jh._internal_counts.num_finished_jobs
    num_skipped_jobs = jh._internal_counts.num_skipped_jobs
    print(f'Num jobs: {num_jobs}')
    print(f'Num submitted jobs: {num_submitted_jobs}')
    print(f'Num errored jobs: {num_errored_jobs}')
    print(f'Num finished jobs: {num_finished_jobs}')
    print(f'Num skipped jobs: {num_skipped_jobs}')
    assert num_skipped_jobs == 0
    assert num_jobs == num_errored_jobs + num_finished_jobs

    with hi.Config(job_cache=jc, job_handler=jh):
        do_test_run_functions()
        hi.wait()
    num_new_jobs = jh._internal_counts.num_jobs - num_jobs
    num_new_submitted_jobs = jh._internal_counts.num_submitted_jobs - num_submitted_jobs
    num_new_errored_jobs = jh._internal_counts.num_errored_jobs - num_errored_jobs
    num_new_finished_jobs = jh._internal_counts.num_finished_jobs - num_finished_jobs
    num_new_skipped_jobs = jh._internal_counts.num_skipped_jobs - num_skipped_jobs
    print(f'Num new jobs: {num_jobs}')
    print(f'Num new submitted jobs: {num_new_submitted_jobs}')
    print(f'Num new errored jobs: {num_new_errored_jobs}')
    print(f'Num new finished jobs: {num_new_finished_jobs}')
    print(f'Num new skipped jobs: {num_new_skipped_jobs}')
    assert num_new_submitted_jobs == num_new_errored_jobs
    assert num_new_jobs == num_new_skipped_jobs + num_errored_jobs

@pytest.mark.container
def test_run_functions_in_container(general):
    with hi.Config(container=True, job_handler=hi.ParallelJobHandler(num_workers=20)):
        do_test_run_functions()

@pytest.mark.container
@pytest.mark.current
def test_slurmjobhandler(general):
    with hi.TemporaryDirectory() as tmpdir:
        working_dir = f'{tmpdir}/slurmjobhandler'
        os.mkdir(working_dir)
        jh = hi.SlurmJobHandler(
            working_dir=working_dir,
            num_workers_per_batch=14,
            num_cores_per_job=2,
            use_slurm=False
        )
        with hi.Config(job_handler=jh, container=True):
            do_test_run_functions()