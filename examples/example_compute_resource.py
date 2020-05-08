#!/usr/bin/env python

import os
import hither as hi

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    db = hi.Database(mongo_url=mongo_url, database='hither')
    jh = hi.ParallelJobHandler(num_workers=8)
    jc = hi.JobCache(database=db)
    CR = hi.ComputeResource(database=db, compute_resource_id='resource1', job_handler=jh, kachery='default_readwrite', job_cache=jc)
    CR.clear()
    CR.run()

if __name__ == '__main__':
    main()