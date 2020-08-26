import hither as hi

@hi.function('load_kachery_text', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def load_kachery_text(uri):
    import kachery as ka
    return ka.load_text(uri)