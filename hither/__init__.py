__version__ = "0.4.2"

from .core import function, container, additional_files, local_modules, opts
from .core import Config
from .core import wait
from .core import reset
from .core import get_function
from ._identity import identity
from ._temporarydirectory import TemporaryDirectory
from ._shellscript import ShellScript
from ._filelock import FileLock
from ._consolecapture import ConsoleCapture
from ._serialize_job import _serialize_job, _deserialize_job
from ._util import _serialize_item, _deserialize_item, _copy_structure_with_changes, _docker_inject_user_dir
from .defaultjobhandler import DefaultJobHandler
from .paralleljobhandler import ParallelJobHandler
from .slurmjobhandler import SlurmJobHandler
from .remotejobhandler import RemoteJobHandler
from .computeresource.computeresource import ComputeResource
from .jobcache import JobCache
from ._enums import JobStatus
from ._exceptions import JobCancelledException, DeserializationException, DuplicateFunctionException
from .noop.noop import noop

# Run a function by name
from .core import run