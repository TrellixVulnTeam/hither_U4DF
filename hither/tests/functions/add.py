import hither as hi

@hi.function('add', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def add(x, y):
    return x + y

def test_calls():
    return [
        dict(
            args=dict(
                x=1, y=2
            ),
            result=3
        )
    ]

add.test_calls = test_calls