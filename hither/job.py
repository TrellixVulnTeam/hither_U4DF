from copy import deepcopy
import time
from typing import List, Dict, Union, Any, Optional

import kachery as ka
from ._Config import Config
from ._consolecapture import ConsoleCapture
from ._containermanager import ContainerManager
from ._enums import JobStatus, JobKeys
from ._exceptions import JobCancelledException
from .file import File
from ._generate_source_code_for_function import _generate_source_code_for_function
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._util import _random_string, _deserialize_item, _serialize_item, _flatten_nested_collection, _copy_structure_with_changes
import kachery_p2p as kp

class Job:
    def __init__(self, *, f, wrapped_function_arguments,
                job_manager, job_handler, job_cache, container, label,
                download_results, job_timeout: Union[float, None],
                force_run: bool, rerun_failing: bool, cache_failing: bool,
                code=None, function_name=None,
                function_version=None, job_id=None, no_resolve_input_files=False):
        self._f = f
        self._code = code
        self._function_name = function_name
        self._function_version = function_version
        self._no_resolve_input_files = no_resolve_input_files
        self._label = label
        self._wrapped_function_arguments = \
            _copy_structure_with_changes(wrapped_function_arguments, File.kache_numpy_array, _as_side_effect=False)
        self._job_id = job_id
        if self._job_id is None:
            self._job_id = _random_string(15)
        self._container = container
        self._download_results = download_results
        self._job_timeout = None
        self._force_run = force_run
        self._rerun_failing = rerun_failing
        self._cache_failing = cache_failing

        self._status = JobStatus.PENDING
        self._result = None
        self._runtime_info: Optional[dict] = None
        self._exception: Union[Exception, None] = None

        self._job_handler = job_handler
        self._job_manager = job_manager
        self._job_cache = job_cache

        # Used by computeresource manager
        self._reported_status = None
        self._handler_uri = None

        # Not used for now
        self._efficiency_job_hash_ = None

        self.flag_remote_file_results_for_download()

# TODO: BREAK THIS DOWN A BIT MORE
    def wait(self, timeout: Union[float, None]=None, resolve_files=True):
        timer = time.time()

        if resolve_files and self._job_handler.is_remote:
            # in this case, we need to make sure that files are downloaded from the remote resource
            if not self._download_results:
                if self._status in JobStatus.prerun_statuses():
                    # it's not too late. Let's just request download now
                    self._download_results = True
                else:
                    # let's wait until finished, and then we'll see what we need to do
                    result = self.wait(timeout=timeout, resolve_files=False)
                    if result is None:
                        return None
                    self._ensure_result_files_are_available_locally(result)
        while True:
            self._job_manager.process_job_queues()
            if self._status == JobStatus.FINISHED:
                if resolve_files:
                    self.resolve_files_in_result()
                return self._result
            elif self._status == JobStatus.ERROR:
                assert self._exception is not None
                raise self._exception
            elif self._status == JobStatus.QUEUED:
                pass
            elif self._status == JobStatus.RUNNING:
                pass
            else:
                raise Exception(f'Unexpected status: {self._status}') # pragma: no cover
            if timeout == 0:
                return None
            time.sleep(0.02)
            elapsed = time.time() - timer
            # Not the same as the job timeout... this is the wait timeout
            if timeout is not None and elapsed > timeout:
                return None

    def get_status(self) -> JobStatus:
        return self._status
    
    def get_label(self) -> str:
        return self._label
    
    def get_function_name(self) -> str:
        return self._function_name
    
    def get_function_version(self) -> str:
        return self._function_version

    def _has_been_submitted(self) -> bool:
        return not self._status in JobStatus.prerun_statuses()

    def _ensure_result_files_are_available_locally(self, results: Any = None):
        """Check  whether the File-type objects in `results` are stored in the local kachery. If not, retrieve them from the kachery-p2p network.
        """
        actual_result = self._result if results is None else results
        result_items = _flatten_nested_collection(actual_result, _type=File)
        for item in result_items:
            assert isinstance(item, File), "Filter failed."
            info = ka.get_file_info(item._kachery_uri, fr=None)
            if info is None:
                if kp.load_file(item._kachery_uri) is None:
                    raise Exception(f'Unable to load result file: {item._kachery_uri}')

    def _result_files_are_available_locally(self, results: Any = None) -> bool:
        """Indicates whether the File-type objects in `results` are stored in the local kachery.

        Keyword Arguments:
            results {Any} -- If specified, an arbitrary collection structure representing
            the results of a Job. If not specified (default), this Job's _results will be
            used. (default: {None})

        Returns:
            bool -- True if all File objects in the result are in the local kachery, else False.
        """
        actual_result = self._result if results is None else results
        result_items = _flatten_nested_collection(actual_result, _type=File)
        for item in result_items:
            assert isinstance(item, File), "Filter failed."
            info = ka.get_file_info(item._kachery_uri, fr=None)
            if info is None:
                return False
        return True

    def ensure_job_results_available_locally(self, job: Any) -> Any:
        """Ensures that all results produced by Jobs that the present Job depends upon
        will be available locally.

        Arguments:
            job {Any} -- Any data element in the inputs to the function wrapped by this
            Job, although we will only operate on other Jobs.

        Returns:
            Any -- The unmodified input, for non-Job or locally-run Job inputs. If the
            input is a Job to be run remotely, we return it either modified to download
            its results, or replace it with a substitute Job which downloads them.
        """
        # Skip anything that is not a remotely-run Job.
        if not isinstance(job, Job) or not job._job_handler.is_remote:
            return job
        if not job._has_been_submitted():
            job._download_results = True
            return job
        else:
            # job has already been submitted. To get its results downloaded, we will
            # replace this job with one that just reads the old job's results, and has
            # the "download me" bit set.
            from ._identity import identity
            with Config(job_handler=job._job_handler, download_results=True, container=True):
                return identity.run(x=job)

    def flag_remote_file_results_for_download(self) -> None:
        """The 'wrapped function arguments' for this function may include the results of
        other Jobs. If this Job is being run with a local job handler, and it depends on
        the results of remotely-run Jobs, we need to make sure their results are loaded
        into Kachery so they are available when this Job runs. This method flags any such
        files for automatic download.
        """
        # If no job handler is assigned, or the job handler is remote, nothing needs to be done.
        if self._job_handler is None or self._job_handler.is_remote:
            return
        # Here we have a local job handler. In this case, iterate over our wrapped inputs, and if
        # any are Jobs being run remotely, set to download their files.
        # In the event they've already run, replace those Jobs with a dummy job that will just
        # download the files (to make sure that result gets cached).
        _copy_structure_with_changes(self._wrapped_function_arguments, self.ensure_job_results_available_locally,
                                    _type = Job, _as_side_effect = False)

    def get_result(self):
        if self._status == JobStatus.FINISHED:
            return self._result
        raise Exception('Cannot get result of job that is not yet finished.')

    def get_exception(self):
        if self._status == JobStatus.ERROR:
            assert self._exception is not None
        return self._exception

    def set_label(self, label):
        self._label = label
        return self

    def get_runtime_info(self) -> Optional[dict]:
        if self._runtime_info is None:
            return None
        return deepcopy(self._runtime_info)
    
    def print_console_out(self) -> None:
        if self._status not in JobStatus.complete_statuses():
            # don't print anything if the job is not complete
            return
        runtime_info = self.get_runtime_info()
        assert runtime_info is not None
        assert runtime_info['console_out']
        _print_console_out(runtime_info['console_out'])
    
    def cancel(self):
        assert self._job_handler is not None, 'Cannot cancel a job that does not have a job handler'
        self._job_handler.cancel_job(job_id=self._job_id)

    def _execute(self, cancel_filepath=None):
        # Note that cancel_filepath will only have an effect if we are running this in a container
        if self._container is not None:
            job_serialized = self._serialize(generate_code=True)
            success, result, runtime_info, error = _run_serialized_job_in_container(job_serialized, cancel_filepath=cancel_filepath)
            self._runtime_info = runtime_info
            if success:
                self._result = result
                self._status = JobStatus.FINISHED
            else:
                assert error is not None
                assert error != 'None'
                if error == JobKeys.CANCELLED_FLAG:
                    self._exception = JobCancelledException('Job was cancelled')
                else:
                    self._exception = Exception(error)
                self._status = JobStatus.ERROR
        else:
            assert self._f is not None, 'Cannot execute job outside of container when function is not available'
            try:
                if not self._no_resolve_input_files:
                    # important not to modify wrapped_function_arguments
                    args0 = _copy_structure_with_changes(self._wrapped_function_arguments, lambda r: r.resolve(), _type = File, _as_side_effect = False)
                else:
                    args0 = self._wrapped_function_arguments
                with ConsoleCapture(label=self.get_label(), show_console=True) as cc:
                    ret = self._f(**args0)
                self._runtime_info = cc.runtime_info()
                self._result = _copy_structure_with_changes(ret, File.kache_numpy_array, _as_side_effect=False)
                # self._result = _deserialize_item(_serialize_item(ret))
                self._status = JobStatus.FINISHED
            except Exception as e:
                self._status = JobStatus.ERROR
                self._exception = e

    def _efficiency_job_hash(self):
        # For purpose of efficiently handling the exact same job queued multiple times simultaneously
        # Important: this is NOT the hash used to lookup previously-run jobs in the cache
        # NOTE: this is not used for now
        if self._efficiency_job_hash_ is not None:
            return self._efficiency_job_hash_
        efficiency_job_hash_obj = dict(
            function_name=self._function_name,
            function_version=self._function_version,
            kwargs=_serialize_item(self._wrapped_function_arguments),
            container=self._container,
            download_results=self._download_results,
            job_timeout=self._job_timeout,
            force_run=self._force_run,
            rerun_failing=self._rerun_failing,
            cache_failing=self._cache_failing,
            no_resolve_input_files=self._no_resolve_input_files
        )
        self._efficiency_job_hash_ = ka.get_object_hash(efficiency_job_hash_obj)
        return self._efficiency_job_hash_

    def download_results_if_needed(self) -> None:
        if self._job_handler.is_remote:
            for f in _flatten_nested_collection(self._result, _type=File):
                assert isinstance(f, File), "Filter failed."
                assert kp.load_file(f._kachery_uri) is not None, f'Unable to load file: {f._kachery_uri}'

    def download_parameter_files_if_needed(self) -> None:
        for a in _flatten_nested_collection(self._wrapped_function_arguments, _type=File):
            assert isinstance(a, File), "Filter failed."
            assert kp.load_file(a._kachery_uri) is not None, f'Unable to load file: {a._kachery_uri}'

    # TODO: Make this part of the .result() method? Would need to access info about
    # the "don't-resolve-results" parameter.
    def resolve_files_in_result(self) -> None:
        """Handles file availability and unboxing of numpy arrays from Kachery files for
        items in the Job's result.
        """
        self._result = _copy_structure_with_changes(self._result,
            lambda r: r.resolve(), _type = File, _as_side_effect = False)

    # TODO: What guarantee do we have that these are actually all complete? Should have a check for it
    def resolve_wrapped_job_values(self) -> None:
        self._wrapped_function_arguments = \
            _copy_structure_with_changes(self._wrapped_function_arguments,
                lambda arg: arg.get_result(), _type = Job, _as_side_effect = False)


    def is_ready_to_run(self) -> bool:
        """Checks current status and status of Jobs this Job depends on, to determine whether this
        Job can be run.

        Raises:
            NotImplementedError: For _same_hash_as functionality.

        Returns:
            bool -- True if this Job can be run (or depends on an errored Job such that it will
            never run successfully); False if it might run in the future and should wait further.
        """
        if hasattr(self, '_same_hash_as'):
            raise NotImplementedError # TODO: this
        if self._status not in [JobStatus.QUEUED, JobStatus.ERROR]: return False
        wrapped_jobs: List[Job] = _flatten_nested_collection(self._wrapped_function_arguments, _type=Job)
        # Check if we depend on any Job that's in error status. If we do, we are in error status,
        # since that dependency is now unresolvable
        errored_jobs: List[Job] = [e for e in wrapped_jobs if e._status == JobStatus.ERROR]
        if errored_jobs:
            self.unwrap_error_from_wrapped_job()
            return True

        # If any job we depend on is still incomplete, we are not ready to run
        incomplete_jobs: List[Job] = [j for j in wrapped_jobs if j._status in JobStatus.incomplete_statuses()]
        if incomplete_jobs:
            return False

        # in the absence of any Job dependency issues, assume we are ready to run
        return True

    def unwrap_error_from_wrapped_job(self) -> None:
        """If any Job this Job depends on has an error status, set own status to error and bubble up
        the content of the error from an arbitrarily chosen inner Job.
        Avoid overwriting any existing errors. If no depended-upon Job is in an error status, do nothing.
        """
        if self._exception is not None:
            self._status = JobStatus.ERROR
            return             # don't overwrite an existing error
        wrapped_jobs: List[Job] = _flatten_nested_collection(self._wrapped_function_arguments, _type=Job)
        errored_jobs: List[Job] = [e for e in wrapped_jobs if e._status == JobStatus.ERROR]
        if not errored_jobs: return
        self._status = JobStatus.ERROR
        self._exception = Exception(f'Exception in wrapped Job: {str(errored_jobs[0]._exception)}')

    def prepare_container_if_needed(self) -> None:
        """Calls global container manager to ensure container images are downloaded, if a container is
        required for the Job. On container fetch error, set error status and record the exception in the Job.
        """
        # No need to prepare a container if none was specified
        if self._container is None: return
        # If we are attached to a remote job handler, the container is actually needed on the
        # remote resource, not the machine where we're currently executing. Don't prepare anything.
        if self._job_handler is not None and self._job_handler.is_remote: return
        try:
            ContainerManager.prepare_container(self._container)
        except:
            self._status = JobStatus.ERROR
            self._exception = Exception(f"Unable to prepare container for Job {self._label}: {self._container}")

    def _compute_hash(self) -> str:
        hash_object = {
            JobKeys.FUNCTION_NAME: self._function_name,
            JobKeys.FUNCTION_VERSION: self._function_version,
            JobKeys.WRAPPED_ARGS: _serialize_item(self._wrapped_function_arguments)
        }
        if self._no_resolve_input_files:
            hash_object[JobKeys.NO_RESOLVE_INPUT_FILES] = True
        return ka.get_object_hash(hash_object)

    def _as_cached_result(self) -> Dict[str, Any]:
        from .computeresource import _result_small_enough_to_store_directly
        cached_result = {
            JobKeys.JOB_HASH: self._compute_hash(),
            JobKeys.STATUS: self._status.value,
            JobKeys.RUNTIME_INFO: self._runtime_info,
            JobKeys.EXCEPTION: '{}'.format(self._exception)
        }
        serialized_result = self._serialized_result()
        if _result_small_enough_to_store_directly(serialized_result):
            cached_result[JobKeys.RESULT] = serialized_result
        else:
            cached_result[JobKeys.RESULT_URI] = kp.store_object(dict(result=serialized_result))
        return cached_result
    
    def _serialized_result(self) -> Any:
        return _serialize_item(self._result)

    def _serialize(self, generate_code:bool):
        function_name = self._function_name
        function_version = self._function_version
        if generate_code:
            if self._code is not None:
                code = self._code
            else:
                assert self._f is not None, 'Cannot serialize function with generate_code=True when function and code are both not available'
                # only generate code once per function
                if not hasattr(self._f, '_hither_generated_code'):
                    code0 = _generate_source_code_for_function(self._f)
                    setattr(self._f, '_hither_generated_code', code0)
                code = getattr(self._f, '_hither_generated_code')
            function = None
        else:
            assert self._f is not None, 'Cannot serialize function with generate_code=False when function is not available'
            code = None
            function = self._f
        x = {
            JobKeys.JOB_ID: self._job_id,
            JobKeys.FUNCTION: function,
            JobKeys.CODE: code,
            JobKeys.FUNCTION_NAME: function_name,
            JobKeys.FUNCTION_VERSION: function_version,
            JobKeys.LABEL: self._label,
            JobKeys.WRAPPED_ARGS:_serialize_item(self._wrapped_function_arguments),
            JobKeys.CONTAINER: self._container,
            JobKeys.DOWNLOAD_RESULTS: self._download_results,
            JobKeys.JOB_TIMEOUT: self._job_timeout,
            JobKeys.FORCE_RUN: self._force_run,
            JobKeys.RERUN_FAILING: self._rerun_failing,
            JobKeys.CACHE_FAILING: self._cache_failing,
            JobKeys.NO_RESOLVE_INPUT_FILES: self._no_resolve_input_files
        }
        x = _serialize_item(x, require_jsonable=False)
        return x
    
    # TODO: Consider moving into the Database file?
    @staticmethod
    def _deserialize(serialized_job, job_manager=None):
        j = serialized_job
        return Job(
            f=j[JobKeys.FUNCTION],
            code=j[JobKeys.CODE],
            function_name=j[JobKeys.FUNCTION_NAME],
            function_version=j[JobKeys.FUNCTION_VERSION],
            label=j[JobKeys.LABEL],
            wrapped_function_arguments=_deserialize_item(j[JobKeys.WRAPPED_ARGS]),
            container=j[JobKeys.CONTAINER],
            download_results=j.get(JobKeys.DOWNLOAD_RESULTS, False),
            job_timeout=j.get(JobKeys.JOB_TIMEOUT, None),
            force_run=j.get(JobKeys.FORCE_RUN, False),
            rerun_failing=j.get(JobKeys.RERUN_FAILING, False),
            cache_failing=j.get(JobKeys.CACHE_FAILING),
            job_manager=job_manager,
            job_handler=None,
            job_cache=None,
            job_id=j[JobKeys.JOB_ID],
            no_resolve_input_files=j[JobKeys.NO_RESOLVE_INPUT_FILES]
        )

def _print_console_out(x):
    for a in x['lines']:
        t = _fmt_time(a['timestamp'])
        txt = a['text']
        print(f'{t}: {txt}')

def _fmt_time(t):
    import datetime
    return datetime.datetime.fromtimestamp(t).isoformat()