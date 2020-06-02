import os
import hither as hi
import numpy as np

@hi.function('additional_file', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
@hi.additional_files(['test_data.csv'])
def additional_file():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    a = np.loadtxt(thisdir + '/test_data.csv', delimiter=',')
    assert a.shape == (2, 3)
    return a

def test_calls():
    return [
        dict(
            args=dict(),
            result=np.array([[1, 2, 3], [4, 5, 6]])
        )
    ]

additional_file.test_calls = test_calls