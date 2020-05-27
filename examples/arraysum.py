# arraysum.py

import hither as hi
import time as time

@hi.function('arraysum', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:dc57157d6316')
def arraysum(x):
    import numpy as np
    return np.sum(x)