import time
import hither as hi

@hi.function('intentional_error', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def intentional_error(delay=None):
    if delay is not None:
        time.sleep(delay)
    raise Exception('intentional-error')

def test_calls():
    return [
        dict(
            args=dict(
                delay=0
            ),
            exception=Exception('intentional-error')
        )
    ]

intentional_error.test_calls = test_calls