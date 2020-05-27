import os
import hither as hi

@hi.function('read_text_file', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def read_text_file(file):
    with open(file, 'r') as f:
        return f.read()

def test_calls():
    thisdir = os.path.dirname(os.path.realpath(__file__))
    return [
        dict(
            args=dict(
                file=hi.File(thisdir + '/test_text.txt')
            ),
            result='some-text'
        )
    ]

read_text_file.test_calls = test_calls