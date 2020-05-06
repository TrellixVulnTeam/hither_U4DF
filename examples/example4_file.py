#!/usr/bin/env python

import os
import numpy as np
import hither2 as hi
import kachery as ka
import time

@hi.function('readnpy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def readnpy(x):
    print(x)
    return np.load(ka.load_file(x))

@hi.function('make_zeros_npy', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def make_zeros_npy(shape):
    x = np.zeros(shape)
    with hi.TemporaryDirectory() as tmpdir:
        fname = tmpdir + '/tmp.npy'
        np.save(fname, x)
        return hi.File(fname)

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    db = hi.Database(mongo_url=mongo_url, database='hither2')

    cache = hi.JobCache(database=db, force_run=True)
    with hi.Config(job_handler=hi.ParallelJobHandler(num_workers=8), container=False, job_cache=cache):
        f = make_zeros_npy.run(shape=(5, 2)).wait()
        a = readnpy.run(x=f).wait()
        print(a)
    
    cache = hi.JobCache(database=db, force_run=False)
    with hi.Config(job_handler=hi.ParallelJobHandler(num_workers=8), container=False, job_cache=cache):
        f = make_zeros_npy.run(shape=(5, 2)).wait()
        a = readnpy.run(x=f).wait()
        print(a)

if __name__ == '__main__':
    main()