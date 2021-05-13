import time
import os
import hither2 as hi
import multiprocessing

thisdir = os.path.dirname(os.path.realpath(__file__))
@hi.function(
    'test1', '0.1.0',
    image=hi.DockerImageFromScript(dockerfile=f'{thisdir}/example_functions/docker/Dockerfile.numpy', name='magland/numpy')
)
def test1():
    print('test1')
    p = multiprocessing.Process(target=test_process)
    # p.daemon = True
    p.start()
    return 3

def test_process():
    for j in range(6):
        time.sleep(1)
        print('x')

if __name__ == '__main__':
    with hi.Config(use_container=True, show_console=True):
        x = test1.run().wait().return_value
    print(x)
