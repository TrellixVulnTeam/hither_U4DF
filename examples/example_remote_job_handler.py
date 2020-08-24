import os
import hither as hi
from integrate_bessel import integrate_bessel

# Adjust as needed
compute_resource_uri = os.environ['COMPUTE_RESOURCE_URI']

# Configure hither to use this job handler
with hi.RemoteJobHandler(compute_resource_uri=compute_resource_uri) as jh:
    with hi.Config(job_handler=jh, container=True):
        x = integrate_bessel.run(v=2.5, a=0, b=4.5).wait()
        print(f'Result = {x}')