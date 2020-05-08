import hither as hi

@hi.function('bad_container', '0.1.0')
@hi.container('docker://bad/container-name')
def bad_container():
    pass

def test_calls():
    return [
        dict(
            args=dict(),
            exception=True,
            container_only=True
        )
    ]

bad_container.test_calls = test_calls

