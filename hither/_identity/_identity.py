import hither as hi

@hi.function('identity', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
@hi.opts(no_resolve_input_files=True)
def identity(x):
    return x