#!/usr/bin/env python

import os
import numpy as np
import hither2 as hi
import time

@hi.function('readnpy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def readnpy(x):
    return np.load(x)

@hi.function('make_zeros_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def make_zeros_npy(shape):
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

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    db = hi.Database(mongo_url=mongo_url, database='hither2')
    cache = hi.JobCache(database=db)
    with hi.Config(job_cache=cache):
        f = make_zeros_npy.run(shape=(6, 3))
        with hi.Config(job_handler=hi.RemoteJobHandler(database=db, compute_resource_id='resource1'), container=True):
            with hi.Config(download_results=True):
                g = add_one_npy.run(x=f)
        a = readnpy.run(x=g)
    print(a.wait())

if __name__ == '__main__':
    main()