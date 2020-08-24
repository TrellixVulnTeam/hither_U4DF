import hither as hi

@hi.function('identity', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def identity(x):
    return x