# expensive_calculation.py

import hither as hi
import time as time

@hi.function('expensive_calculation', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def expensive_calculation(x):
    # Simulate an expensive computation by just sleeping for
    # x seconds and then return 42
    time.sleep(x)
    return 42