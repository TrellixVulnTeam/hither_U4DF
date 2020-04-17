import os
import hither2 as hi

@hi.function('read_text_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def read_text_file(file):
    with open(file, 'r') as f:
        return f.read()

thisdir = os.path.dirname(os.path.realpath(__file__))
read_text_file.test_calls = [
    dict(
        args=dict(
            file=thisdir + '/test_text.txt'
        ),
        result='some-text'
    )
]