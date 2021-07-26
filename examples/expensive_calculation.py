# expensive_calculation.py

import hither2 as hi
import time as time

@hi.function(
    'expensive_calculation', '0.1.0',
    image=hi.RemoteDockerImage('docker://jsoules/simplescipy:latest'),
    modules=['simplejson'],
)
def expensive_calculation(x):
    # Simulate an expensive computation by just sleeping for
    # x seconds and then return 42
    time.sleep(x)
    return 42