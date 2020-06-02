import os
import hither as hi

@hi.function('write_text_file', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def write_text_file(text):
    with hi.TemporaryDirectory() as tmpddir:
        fname = tmpddir + '/file.txt'
        with open(fname, 'w') as f:
            f.write(text)
        return hi.File(fname)

def test_calls():
    return [
        dict(
            args=dict(
                text='test-write-text-file'
            )
        )
    ]

write_text_file.test_calls = test_calls