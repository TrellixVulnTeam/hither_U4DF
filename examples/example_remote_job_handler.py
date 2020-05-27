import os
import hither as hi
from integrate_bessel import integrate_bessel

# Adjust as needed
port = 15402

# Adjust as needed
compute_resource_id = os.environ['COMPUTE_RESOURCE_ID']

# Create the remote job handler
jh = hi.RemoteJobHandler(
    event_stream_client=hi.EventStreamClient(
        url=f'http://localhost:{port}',
        channel='readwrite',
        password='readwrite'
    ),
    compute_resource_id=compute_resource_id
)

# Configure hither to use this job handler
with hi.Config(job_handler=jh, container=True):
    x = integrate_bessel.run(v=2.5, a=0, b=4.5).wait()
    print(f'Result = {x}')