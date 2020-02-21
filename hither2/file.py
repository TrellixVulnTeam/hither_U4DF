from os import stat
import kachery as ka

class File:
    def __init__(self, path):
        self._sha1_path = ka.store_file(path)
        self.path = self._sha1_path
    def serialize(self):
        return dict(
            _type='hither2_file',
            sha1_path=self._sha1_path
        )
    @staticmethod
    def can_deserialize(x):
        if type(x) != dict:
            return False
        return (x.get('_type', None) == 'hither2_file') and ('sha1_path' in x)
    @staticmethod
    def deserialize(x):
        return File(x['sha1_path'])