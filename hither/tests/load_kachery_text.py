import hither as hi

@hi.function('load_kachery_text', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
def load_kachery_text(uri):
    import kachery_p2p as kp
    return kp.load_text(uri)