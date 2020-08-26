import os
import hither as hi
import numpy as np

@hi.function('identity2', '0.1.1')
@hi.container('docker://jsoules/simplescipy:latest')
def identity2(x):
    return x

def test_calls():
    # thisdir = os.path.dirname(os.path.realpath(__file__))
    return [
        dict(
            args=dict(
                x=x
            ),
            result=x
        ) for x in [
            # thisdir + '/identity.py',
            dict(a=3),
            [1, 2, 3],
            (1, 2, 3),
            np.array([4, 5, 6])
        ]
    ]

identity2.test_calls = test_calls