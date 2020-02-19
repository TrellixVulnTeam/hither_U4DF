#!/usr/bin/env python

import numpy as np
import hither2 as hi

@hi.function('sumsqr', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:latest')
def sumsqr(x):
    return np.sum(x**2)

@hi.function('addone', '0.1.0')
def addone(x):
    return x + 1

@hi.function('addem', '0.1.0')
def addem(x):
    return np.sum(x)

def main():
    with hi.config(container=True):
    # with hi.config(container=False):
        val1 = sumsqr.run(x=np.array([1,2,3]))
    val2 = addone.run(x=val1)
    val3 = addem.run(x=[val1, val2])
    print(val3.wait())
    print(val1.wait(), val2.wait(), val3.wait())

if __name__ == '__main__':
    main()