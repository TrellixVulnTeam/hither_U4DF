import hither as hi

# a no-operation hither function
@hi.function('noop', '0.1.0')
@hi.container('docker://python:3.7.9')
def noop():
    pass