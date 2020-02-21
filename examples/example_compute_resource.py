#!/usr/bin/env python

import os
import hither2 as hi

def main():
    mongo_url = os.getenv('MONGO_URL', 'mongodb://localhost:27017')
    jh = hi.ParallelJobHandler(num_workers=8)
    CR = hi.ComputeResource(mongo_url=mongo_url, database='hither2', compute_resource_id='resource1', job_handler=jh)
    CR.clear()
    CR.run()

if __name__ == '__main__':
    main()