import time
import numpy as np
import hither2 as hi

@hi.function('readnpy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def readnpy(x):
    return np.load(x)

@hi.function('make_zeros_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def make_zeros_npy(shape, delay=None):
    if delay is not None:
        time.sleep(delay)
    x = np.zeros(shape)
    with hi.TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/tmp.npy'
        np.save(fname, x)
        return hi.File(fname)

@hi.function('add_one_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def add_one_npy(x):
    x = np.load(x)
    with hi.TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/tmp.npy'
        np.save(fname, x + 1)
        return hi.File(fname)