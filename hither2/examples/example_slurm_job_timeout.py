import os
from hither2 import dockerimagefromscript
import time
from typing import List, Union
import hither2 as hi
import numpy as np

thisdir = os.path.dirname(os.path.realpath(__file__))

def main():
    test_sing()

# @hi.function(
#     'test_numpy_serialization2', '0.1.0',
#     image=hi.RemoteDockerImage(name='magland/numpy', tag='latest'),
#     modules=[]
# )
@hi.function(
    'test_numpy_serialization2', '0.1.0',
    image=hi.DockerImageFromScript(dockerfile=f'{thisdir}/example_functions/docker/Dockerfile.numpy', name='magland/numpy'),
    modules=[]
)
def test_numpy_serialization2(*, x: np.ndarray, delay: Union[float, None]=None):
    print(f'nps {x.shape} {delay}')
    if delay is not None:
        time.sleep(delay)
    return x, x * 2

@hi.function('test_id', '0.1.0')
def test_id(x):
    print(f'+++id+++')
    return x

def test_sing():
    jh = hi.SlurmJobHandler(num_jobs_per_allocation=4, max_simultaneous_allocations=3, srun_command='sleep 4 && ')
    # jh = hi.ParallelJobHandler(num_workers=4)
    log = hi.Log()
    a = np.array([1, 2, 3, 4, 5])
    with hi.Config(use_container=True, job_handler=jh, log=log, job_timeout_sec=3):
        jobs = [
            hi.Job(test_numpy_serialization2, dict(x=a*i, delay=5))
            for i in range(20)
        ]
        j2 = hi.Job(test_id, {'x': jobs})
        print('*******************************************')
        cc = j2.wait().return_value
        print(cc)
        for j in jobs:
            j.print_console()

if __name__ == '__main__':
    main()