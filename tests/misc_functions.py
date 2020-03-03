from types import SimpleNamespace
import os
import time
import numpy as np
import hither2 as hi

@hi.function('zeros', '0.1.1')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def zeros(shape, delay=None):
    if delay is not None:
        time.sleep(delay)
    return np.zeros(shape=shape)

@hi.function('ones', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def ones(shape):
    return np.ones(shape=shape)

@hi.function('add', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def add(x, y):
    return x + y

@hi.function('mult', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def mult(x, y):
    return x * y

@hi.function('write_text_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def write_text_file(text):
    with hi.TemporaryDirectory() as tmpddir:
        fname = tmpddir + '/file.txt'
        with open(fname, 'w') as f:
            f.write(text)
        return hi.File(fname)

@hi.function('read_text_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def read_text_file(file):
    with open(file, 'r') as f:
        return f.read()

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

@hi.function('identity', '0.1.1')
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

misc_functions = SimpleNamespace(
    zeros=zeros,
    ones=ones,
    add=add,
    mult=mult,
    write_text_file=write_text_file,
    read_text_file=read_text_file,
    intentional_error=intentional_error,
    do_nothing=do_nothing,
    bad_container=bad_container,
    additional_file=additional_file,
    local_module=local_module,
    identity=identity
)

