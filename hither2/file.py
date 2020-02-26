from os import stat
import kachery as ka

class File:
    def __init__(self, path):
        if path.startswith('sha1://') or path.startswith('sha1dir://'):
            self._sha1_path = path
        else:
            self._sha1_path = ka.store_file(path)
        self.path = self._sha1_path
    def serialize(self):
        ret = dict(
            _type='hither2_file',
            sha1_path=self._sha1_path,
        )
        return ret
    @staticmethod
    def can_deserialize(x):
        if type(x) != dict:
            return False
        return (x.get('_type', None) == 'hither2_file') and ('sha1_path' in x)
    @staticmethod
    def deserialize(x):
        return File(x['sha1_path'])