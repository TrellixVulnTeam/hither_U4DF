__version__ = "0.3.3"

from .core import function, container, additional_files, local_modules, opts
from .core import Config
from .core import wait
from .core import reset
from ._identity import identity
from ._temporarydirectory import TemporaryDirectory
from ._shellscript import ShellScript
from ._exceptions import JobCancelledException
from ._filelock import FileLock
from ._consolecapture import ConsoleCapture
from .core import _deserialize_job
from ._util import _serialize_item, _deserialize_item, _copy_structure_with_changes, _docker_inject_user_dir
from .defaultjobhandler import DefaultJobHandler
from .paralleljobhandler import ParallelJobHandler
from .slurmjobhandler import SlurmJobHandler
from .remotejobhandler import RemoteJobHandler
from .computeresource import ComputeResource
from .database import Database
from .jobcache import JobCache
from ._enums import JobStatus, HitherFileType
from .file import File
from ._exceptions import JobCancelledException, DeserializationException, DuplicateFunctionException

# Run a function by name
from .core import run