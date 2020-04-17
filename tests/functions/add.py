import hither2 as hi

@hi.function('add', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def add(x, y):
    return x + y

add.test_calls = [
    dict(
        args=dict(
            x=1, y=2
        ),
        result=3
    )
]