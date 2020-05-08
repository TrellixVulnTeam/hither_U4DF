import hither as hi

@hi.function('identity', '0.1.0')
# TODO: find a more appropriate container for the identity function (note: we need numpy for hither... but maybe that shouldn't be a requirement)
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
@hi.opts(no_resolve_input_files=True)
def identity(x):
    return x