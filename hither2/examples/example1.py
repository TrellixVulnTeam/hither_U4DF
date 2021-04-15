import time
from typing import List
from hither.job import Job
import hither2 as hi
import numpy as np
from example_functions.test_numpy_serialization import test_numpy_serialization

def main():
    test1()
    test2()
    test3()
    test4()
    test5()

def test1():
    a = np.array([[1, 2, 3], [4, 5, 6 + 7j]])
    b, c = test_numpy_serialization(x=a)
    print(b)
    print(c)

    b, c = test_numpy_serialization(x=a)
    print(b)
    print(c)

@hi.function('test_id', '0.1.0')
def test_id(x):
    return x

def test2():
    a = np.array([1, 2, 3, 4, 5])
    with hi.Config(use_container=True, show_console=True):
        j = hi.Job(test_numpy_serialization, dict(x=a))
        j2 = hi.Job(test_id, dict(x=j))
        print('*******************************************')
        r = j2.wait()
        b, c = r.return_value
        print(b)
        print(c)

def test3():
    jh = hi.ParallelJobHandler(num_workers=4)
    a = np.array([1, 2, 3, 4, 5])
    with hi.Config(use_container=True, job_handler=jh):
        jobs = [
            hi.Job(test_numpy_serialization, dict(x=a*i, delay=3))
            for i in range(4)
        ]
        j2 = hi.Job(test_id, {'x': jobs})
        print('*******************************************')
        cc = j2.wait().return_value
        print(cc)

def test4():
    a = np.array([1, 2, 3, 4, 5])
    jc = hi.JobCache(feed_name='default-job-cache')
    with hi.Config(use_container=True, job_cache=jc):
        j = hi.Job(test_numpy_serialization, dict(x=a))
        j2 = hi.Job(test_id, dict(x=j))
        print('*******************************************')
        r = j2.wait()
        b, c = r.return_value
        print(b)
        print(c)

@hi.function('multiply_arrays', '0.1.2')
def multiply_arrays(x: np.ndarray, y: np.ndarray, delay: float):
    if delay > 0: time.sleep(delay)
    return x * y

def test5():
    jc = hi.JobCache(feed_name='default-job-cache')
    jh = hi.ParallelJobHandler(num_workers=4)
    jobs: List[hi.Job] = []
    with hi.Config(job_cache=jc, job_handler=jh):
        for i in range(8):
            print(f'Creating job {i}')
            j = hi.Job(multiply_arrays, dict(x=np.array([i, i]), y=np.array([2, 2]), delay=4))
            jobs.append(j)
    print('Waiting for jobs to complete')
    hi.wait(None)
    for j in jobs:
        if j.status == 'finished':
            print('RESULT:', j.status, j.result.return_value)
        elif j.status == 'error':
            print('ERROR', j.result.error)

if __name__ == '__main__':
    main()