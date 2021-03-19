import os
import numpy as np
import hither2 as hi2

thisdir = os.path.dirname(os.path.realpath(__file__))

@hi2.function(
    'test_numpy_serialization', '0.1.0',
    image=hi2.LocalDockerImage(name='numpy', dockerfile=f'{thisdir}/docker/Dockerfile.numpy'),
    modules=[]
)
def test_numpy_serialization(*, x: np.ndarray):
    return x, x * 2