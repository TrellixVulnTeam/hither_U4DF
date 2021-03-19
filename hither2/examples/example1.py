import hither2 as hi2
import numpy as np
from example_functions.test_numpy_serialization import test_numpy_serialization

def main():
    a = np.array([1, 2, 3, 4])
    b, c = test_numpy_serialization(x=a)
    print(b)
    print(c)

    with hi2.Config(use_container=True):
        b, c = test_numpy_serialization(x=a)
        print(b)
        print(c)

if __name__ == '__main__':
    main()