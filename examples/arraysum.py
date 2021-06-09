# arraysum.py

import hither2 as hi

@hi.function('arraysum', '0.1.0', container='docker://jsoules/simplescipy:latest')
# @hi.container('docker://jupyter/scipy-notebook:dc57157d6316')
def arraysum(x):
    import numpy as np
    return np.sum(x)