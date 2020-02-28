import os
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

@hi.function('intentional_error', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def intentional_error(delay=None):
    if delay is not None:
        time.sleep(delay)
    raise Exception('intentional-error')

@hi.function('do_nothing', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def do_nothing(x, delay=None):
    if delay is not None:
        time.sleep(delay)

@hi.function('bad_container', '0.1.0')
@hi.container('docker://bad/container-name')
def bad_container():
    pass

@hi.function('additional_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
@hi.additional_files(['test_data.csv'])
def additional_file():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    a = np.loadtxt(thisdir + '/test_data.csv', delimiter=',')
    assert a.shape == (2, 3)
    return a

@hi.function('local_module', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
@hi.local_modules(['./test_modules/test_module1'])
def local_module():
    import test_module1
    assert test_module1.return42() == 42
    return True