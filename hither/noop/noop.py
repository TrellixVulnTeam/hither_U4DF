import hither as hi

# a no-operation hither function
@hi.function('noop', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def noop():
    pass