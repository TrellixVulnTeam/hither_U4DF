from os import stat
from os.path import basename
from typing import Any, List, Union

import kachery as ka

class File:
    def __init__(self, path, item_type='file'):
        if path.startswith('sha1://') or path.startswith('sha1dir://'):
            self._sha1_path = path
        else:
            self._sha1_path = ka.store_file(path, basename=_get_basename_from_path(path))
        self.path = self._sha1_path
        self._item_type = item_type

    def serialize(self):
        ret = dict(
            _type='hither2_file',
            sha1_path=self._sha1_path,
            item_type=self._item_type
        )
        return ret

    def array(self):
#        import pdb;pdb.set_trace()
        if self._item_type != 'ndarray':
            raise Exception('This file is not of type ndarray')
        x = ka.load_npy(self._sha1_path)
        if x is None:
            raise Exception(f'Unable to load npy file: {self._sha1_path}')
        return x

    def ensure_local_availability(self, kachery_src:Union[str, None] = None) -> None:
        # look for file locally or in the specified remote, if any.
        # If found locally, we're done; if found in the kachery source, this downloads it.
        local_path = ka.load_file(self._sha1_path, fr=kachery_src)
        if local_path is not None:
            return
        # couldn't find it locally, try remote handler if it exists.
        # TODO: fix type-hint grumbles; we can't just import the class b/c of a circular dependency
        remote_handler = getattr(self, '_remote_job_handler', None)
        if remote_handler is None:
            raise Exception(f"Unable to download file: {self._sha1_path} locally or from " +
                f"kachery source '{kachery_src}', and no remote_job_handler is attached to the file.")
        # Remote handler does exist. See if it can find the file.
        remote_path = remote_handler._load_file(self._sha1_path)
        assert remote_path is not None, f"Unable to load file {self._sha1_path} " + \
            f"from remote compute resource: {remote_handler._compute_resource_id}."

    @staticmethod
    def can_deserialize(x):
        if type(x) != dict:
            return False
        return (x.get('_type', None) == 'hither2_file') and ('sha1_path' in x)

    @staticmethod
    def deserialize(x):
        return File(x['sha1_path'], item_type=x.get('item_type', 'file'))

def _get_basename_from_path(path):
    if path.startswith('sha1://'):
        return _get_basename_from_path(path[7:])
    elif path.startswith('sha1dir://'):
        return _get_basename_from_path(path[10:])
    a = path.split('/')
    if len(a) > 1:
        return a[-1]
    return None
