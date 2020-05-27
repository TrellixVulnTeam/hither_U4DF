# arraysum.py

import hither as hi

@hi.function('arraysum', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def arraysum(x):
    import numpy as np
    return np.sum(x)