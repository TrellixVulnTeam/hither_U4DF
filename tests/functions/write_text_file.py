import os
import hither2 as hi

@hi.function('write_text_file', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
def write_text_file(text):
    with hi.TemporaryDirectory() as tmpddir:
        fname = tmpddir + '/file.txt'
        with open(fname, 'w') as f:
            f.write(text)
        return hi.File(fname)

write_text_file.test_calls = [
    dict(
        args=dict(
            text='test-write-text-file'
        )
    )
]