import hither as hi

@hi.function('arraysum', '0.1.0', container='docker://jsoules/simplescipy:latest')
def arraysum(x):
    import numpy as np
    return np.sum(x)

def test_calls():
    return [
        dict(
            args=dict(
                x=[2, 3, 4]
            ),
            result=2 + 3 + 4
        )
    ]

arraysum.test_calls = test_calls