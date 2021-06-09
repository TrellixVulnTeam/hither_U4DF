import time
from typing import List
import hither2 as hi
import numpy as np
from .example_functions.test_numpy_serialization import test_numpy_serialization

def main():
    test2()

@hi.function('test_id', '0.1.0')
def test_id(x):
    return x

def test2():
    a = np.array([1, 2, 3, 4, 5])
    with hi.Config(use_container=True):
        j = hi.Job(test_numpy_serialization, dict(x=a))
        j2 = hi.Job(test_id, dict(x=j))
        print('*******************************************')
        r = j2.wait()
        print('*******************************************')
        b, c = r.return_value
        print(b)
        print(c)

if __name__ == '__main__':
    main()