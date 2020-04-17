import hither2 as hi

@hi.function('bad_container', '0.1.0')
@hi.container('docker://bad/container-name')
def bad_container():
    pass

bad_container.test_calls = [
    dict(
        args=dict(),
        result=None
    )
]

