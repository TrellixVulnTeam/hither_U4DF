import os
import time
import numpy as np
import hither2 as hi

@hi.function('readnpy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def readnpy(x):
    return np.load(x)

@hi.function('make_zeros_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def make_zeros_npy(shape, delay=None):
    if delay is not None:
        time.sleep(delay)
    x = np.zeros(shape)
    with hi.TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/tmp.npy'
        np.save(fname, x)
        return hi.File(fname)

@hi.function('add_one_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def add_one_npy(x):
    x = np.load(x)
    with hi.TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/tmp.npy'
        np.save(fname, x + 1)
        return hi.File(fname)

@hi.function('intentional_error', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def intentional_error(delay=None):
    if delay is not None:
        time.sleep(delay)
    raise Exception('intentional-error')

@hi.function('do_nothing', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def do_nothing(x, delay=None):
    if delay is not None:
        time.sleep(delay)

@hi.function('bad_container', '0.1.0')
@hi.container('docker://bad/container-name')
def bad_container():
    pass

@hi.function('additional_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
@hi.additional_files(['test_data.csv'])
def additional_file():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    a = np.loadtxt(thisdir + '/test_data.csv', delimiter=',')
    assert a.shape == (2, 3)
    return a

@hi.function('local_module', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
@hi.local_modules(['./test_modules/test_module1'])
def local_module():
    import test_module1
    assert test_module1.return42() == 42
    return True

@hi.function('identity', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def identity(x):
    if type(x) == str:
        if x.startswith('/') and os.path.exists(x):
            return hi.File(x)
        else:
            return x
    elif type(x) == dict:
        ret = dict()
        for key, val in x.items():
            ret[key] = identity(val)
        return ret
    elif type(x) == list:
        return [identity(a) for a in x]
    elif type(x) == tuple:
        return tuple([identity(a) for a in x])
    else:
        return x