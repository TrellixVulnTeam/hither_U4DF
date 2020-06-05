import time
import hither as hi
import numpy as np

@hi.function('zeros', '0.1.1')
@hi.container('docker://jsoules/simplescipy:latest')
def zeros(shape, delay=None):
    if delay is not None:
        time.sleep(delay)
    return np.zeros(shape=shape)

def test_calls():
    return [
        dict(
            args=dict(
                shape=(5, 2),
                delay=0
            ),
            result=np.zeros((5, 2))
        )
    ]

zeros.test_calls = test_calls