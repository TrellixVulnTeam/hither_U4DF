from os import read

import hither as hi
import kachery_client as kc


# define an example hither function
@hi.function('add_one', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def add_one(x):
    return x + 1

def main():
    # create a new cache uri
    cache1_uri = create_feed_if_does_not_exist('cache1').get_uri()
    print(f'Cache URI: {cache1_uri}')
    # create the job cache
    jc = hi.JobCache(feed_uri=cache1_uri, readonly=False)
    with hi.Config(job_cache=jc):
        # this will only be run the first time
        y = add_one.run(x=41).wait()
        print(y)

# A convenience method to create a new named feed if it does not already exist
def create_feed_if_does_not_exist(feed_name):
    try:
        f = kc.load_feed(feed_name)
        return f
    except:
        return kc.create_feed(feed_name)

if __name__ == '__main__':
    main()
