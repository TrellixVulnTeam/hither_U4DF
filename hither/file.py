from numpy import ndarray
from os import stat
from os.path import basename
from typing import Any, List, Union

from ._enums import HitherFileType # TODO: Not yet used; hard-to-track errors in serialization
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
            _type='hither_file',
            sha1_path=self._sha1_path,
            item_type=self._item_type
        )
        return ret

# TODO: Ths "item type" field should be replaced with an enum.
    def resolve(self) -> Union[str, ndarray]:
        """Ensure that this file is available in Kachery, if it is of type 'file',
        and if it is a boxed numpy array, replace it with the actual numpy array representation.

        Raises:
            Exception: Thrown if an unrecognized item type exists for the item type.

       Returns:
            Union[str, ndarray] -- Path to the file, if this File represents a file
            tracked by kachery; otherwise a numpy array, if this File represents a
            numpy array that was boxed into a kachery file for inter-resource portability.
        """
        if self._item_type == 'file':
            path = ka.load_file(self._sha1_path)
            assert path is not None, f'Unable to load file: {self._sha1_path} from kachery.'
            return path
        elif self._item_type == 'ndarray':
            return self.array()
        else:
            raise Exception(f'Unexpected item type: {self._item_type}')

    def array(self):
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

    def kache(self, kachery_dest:Union[str, None] = None) -> None:
        """Push this File to the configured Kachery store, or the one specified in the
        input parameter.

        Keyword Arguments:
            kachery_dest {Union[str, None]} -- Kachery store to store the file. (default: {None})
        """
        ka.store_file(self._sha1_path, to=kachery_dest)

    @staticmethod
    def can_deserialize(x: Any) -> bool:
        if type(x) != dict:
            return False
        return (x.get('_type', None) == 'hither_file') and ('sha1_path' in x)

    @staticmethod
    def deserialize(x) -> 'File':
        return File(x['sha1_path'], item_type=x.get('item_type', 'file'))

    @staticmethod
    def kache_numpy_array(x: Any) -> Any:
        if not isinstance(x, ndarray): return x
        return File._kache_numpy_array(x)

    @staticmethod
    def _kache_numpy_array(ary: ndarray) -> 'File':
        path = ka.store_npy(ary)
        return File(path, item_type = 'ndarray')

def _get_basename_from_path(path: str) -> Union[str, None]:
    if path.startswith('sha1://'):
        return _get_basename_from_path(path[7:])
    elif path.startswith('sha1dir://'):
        return _get_basename_from_path(path[10:])
    a = path.split('/')
    if len(a) > 1:
        return a[-1]
    return None
