#!/usr/bin/env python

import hither2 as hi

def main():
    mongo_url = 'mongodb://localhost:27017'
    CR = hi.ComputeResource(mongo_url=mongo_url, database='hither2', compute_resource_id='resource1')
    CR.clear()
    CR.run()

if __name__ == '__main__':
    main()