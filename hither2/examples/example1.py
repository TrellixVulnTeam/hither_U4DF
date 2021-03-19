import hither2 as hi2
import numpy as np
from example_functions.test_numpy_serialization import test_numpy_serialization

def main():
    test1()
    test2()

def test1():
    a = np.array([[1, 2, 3], [4, 5, 6 + 7j]])
    b, c = test_numpy_serialization(x=a)
    print(b)
    print(c)

    with hi2.Config(use_container=True):
        b, c = test_numpy_serialization(x=a)
        print(b)
        print(c)

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

if __name__ == '__main__':
    main()