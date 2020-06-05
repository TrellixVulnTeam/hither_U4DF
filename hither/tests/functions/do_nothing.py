import time
import hither as hi

@hi.function('do_nothing', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def do_nothing(x=None, delay=None):
    print('Running do_nothing')
    if delay is not None:
        time.sleep(delay)

def test_calls():
    return [
        dict(
            args=dict(
                x=1, delay=0
            ),
            result=None
        ),
        dict(
            args=dict(
                x=None, delay=None
            ),
            result=None
        )
    ]

do_nothing.test_calls = test_calls