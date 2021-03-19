from hither.job import Job
import hither2 as hi2
import numpy as np
from example_functions.test_numpy_serialization import test_numpy_serialization

def main():
    test1()
    test2()
    test3()

def test1():
    a = np.array([[1, 2, 3], [4, 5, 6 + 7j]])
    b, c = test_numpy_serialization(x=a)
    print(b)
    print(c)

    with hi2.Config(use_container=True):
        b, c = test_numpy_serialization(x=a)
        print(b)
        print(c)

@hi2.function('test_id', '0.1.0')
def test_id(x):
    return x

def test2():
    a = np.array([1, 2, 3, 4, 5])
    with hi2.Config(use_container=True):
        j = hi2.Job(test_numpy_serialization, dict(x=a))
        j2 = hi2.Job(test_id, dict(x=j))
        print('*******************************************')
        r = j2.wait()
        b, c = r.get_return_value()
        print(b)
        print(c)

def test3():
    jh = hi2.ParallelJobHandler(num_workers=4)
    a = np.array([1, 2, 3, 4, 5])
    with hi2.Config(use_container=True, job_handler=jh):
        jobs = [
            hi2.Job(test_numpy_serialization, dict(x=a*i, delay=3))
            for i in range(4)
        ]
        j2 = hi2.Job(test_id, {'x': jobs})
        print('*******************************************')
        r = j2.wait()
        cc = r.get_return_value()
        print(cc)

def test4():
    a = np.array([1, 2, 3, 4, 5])
    jc = hi2.JobCache(feed_name='default-job-cache')
    with hi2.Config(use_container=True, job_cache=jc):
        j = hi2.Job(test_numpy_serialization, dict(x=a))
        j2 = hi2.Job(test_id, dict(x=j))
        print('*******************************************')
        r = j2.wait()
        b, c = r.get_return_value()
        print(b)
        print(c)

if __name__ == '__main__':
    main()