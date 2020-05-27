import numpy as np
import hither as hi

@hi.function('mult', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def mult(x, y):
    return x * y

def test_calls():
    return [
        dict(
            args=dict(
                x=np.array([1, 2, 3]), y=np.array([-1, -2, -3])
            ),
            result=np.array([-1, -4, -9])
        )
    ]

mult.test_calls = test_calls