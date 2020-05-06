from copy import deepcopy
import os
import sys
import time
from typing import Dict, List, Union, Any

import kachery as ka
from ._Config import Config
from ._enums import JobStatus
from .file import File
from ._generate_source_code_for_function import _generate_source_code_for_function
from .remotejobhandler import RemoteJobHandler
from ._run_serialized_job_in_container import _run_serialized_job_in_container
from ._util import _random_string, _docker_form_of_container_string, _deserialize_item, _serialize_item, _flatten_nested_collection, _replace_values_in_structure



class Job:
    def __init__(self, *, f, wrapped_function_arguments,
                job_manager, job_handler, job_cache, container, label,
                download_results, job_timeout: Union[float, None], code=None, function_name=None,
                function_version=None, job_id=None, no_resolve_input_files=False):
        self._f = f
        self._code = code
        self._function_name = function_name
        self._function_version = function_version
        self._no_resolve_input_files = no_resolve_input_files
        self._label = label
        self._wrapped_function_arguments = \
            _replace_values_in_structure(wrapped_function_arguments, File.kache_numpy_array)
        self._job_id = job_id
        if self._job_id is None:
            self._job_id = _random_string(15)
        self._container = container
        self._download_results = download_results
        self._job_timeout = None

        self._status = JobStatus.PENDING
        self._result = None
        self._runtime_info = None
        self._exception: Union[Exception, None] = None

        self._job_handler = job_handler
        self._job_manager = job_manager
        self._job_cache = job_cache
        
        # this is used by wait() in the case where results were not downloaded
        # and the job has already been sent to the remote compute resource
        self._substitute_job_for_wait: Union[None, Job] = None

        # Not used for now
        self._efficiency_job_hash_ = None

        self.flag_remote_file_results_for_download()

# TODO: BREAK THIS DOWN A BIT MORE
    def wait(self, timeout: Union[float, None]=None, resolve_files=True):
        if resolve_files and self._substitute_job_for_wait is not None:
            return self._substitute_job_for_wait.wait(timeout=timeout, resolve_files=resolve_files)

        timer = time.time()

        if resolve_files and self._job_handler.is_remote:
            # in this case, we need to make sure that files are downloaded from the remote resource
            if not self._download_results:
                if self._status in JobStatus.prerun_statuses():
                    # it's not too late. Let's just request download now
                    self._download_results = True
                else:
                    # here is where we make a substitute job
                    # let's wait until finished, and then we'll see what we need to do
                    result = self.wait(timeout=timeout, resolve_files=False)
                    if result is None:
                        return None
                    if not self.result_files_are_available_locally(result):
                        from ._identity import identity
                        assert self._substitute_job_for_wait is None, 'Unexpected at this point in the code: self._substitute_job_for_wait is not None'
                        with Config(job_handler=self._job_handler, download_results=True):
                            self._substitute_job_for_wait = identity.run(x=result)
                        # compute the remainder timeout for this call to wait()
                        timeout2 = timeout
                        if timeout2 is not None:
                            elapsed = time.time() - timer
                            timeout2 = max(0, timeout2 - elapsed)
                        return self._substitute_job_for_wait.wait(timeout=timeout2, resolve_files=resolve_files)
                    else:
                        return self.resolve_files_in_result()
        while True:
            self._job_manager.update_job_statuses()
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

    def status(self) -> JobStatus:
        return self._status

    def has_been_submitted(self) -> bool:
        return not self._status in JobStatus.prerun_statuses()

    def result_files_are_available_locally(self, results: Any = None) -> bool:
        """Indicates whether the File-type objects in `results` have been loaded into kachery.

        Keyword Arguments:
            results {Any} -- If specified, an arbitrary collection structure representing
            the results of a Job. If not specified (default), this Job's _results will be
            used. (default: {None})

        Returns:
            bool -- True if all File objects in the result are in Kachery, else False.
        """
        actual_result = self._result if results is None else results
        result_items = _flatten_nested_collection(actual_result, _type=File)
        for item in result_items:
            assert isinstance(item, File), "Filter failed."
            info = ka.get_file_info(item._sha1_path, fr=None)
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
        if not job.has_been_submitted():
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
        _replace_values_in_structure(self._wrapped_function_arguments, self.ensure_job_results_available_locally)

    def result(self):
        if self._status == JobStatus.FINISHED:
            return self._result
        raise Exception('Cannot get result of job that is not yet finished.')

    def exception(self):
        if self._status == JobStatus.ERROR:
            assert self._exception is not None
        return self._exception

    def set(self, *, label=None):
        if label is not None:
            self._label = label
        return self

    def runtime_info(self):
        return deepcopy(self._runtime_info)

    def _execute(self):
        if self._container is not None:
            job_serialized = self._serialize(generate_code=True)
            success, result, runtime_info, error = _run_serialized_job_in_container(job_serialized)
            self._runtime_info = runtime_info
            if success:
                self._result = result
                self._status = JobStatus.FINISHED
            else:
                assert error is not None
                assert error != 'None'
                self._exception = Exception(error)
                self._status = JobStatus.ERROR
        else:
            assert self._f is not None, 'Cannot execute job outside of container when function is not available'
            try:
                if not self._no_resolve_input_files:
                    self.resolve_files_in_wrapped_arguments()
                ret = self._f(**self._wrapped_function_arguments)
                self._result = _replace_values_in_structure(ret, File.kache_numpy_array)
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
            no_resolve_input_files=self._no_resolve_input_files
        )
        self._efficiency_job_hash_ = ka.get_object_hash(efficiency_job_hash_obj)
        return self._efficiency_job_hash_

    def kache_results_if_needed(self, kachery:Union[str, None] = None) -> None:
        """Upload File-type results to a Kachery server (as indicated by the "Kache" spelling).

        Keyword Arguments:
            kachery {Union[str, None]} -- Specific Kachery instance to push file to.
            (default: {None})
        """
        if not self._download_results: return
        for f in _flatten_nested_collection(self._result, _type=File):
            f.kache(kachery_dest=kachery)

    # TODO: is str the correct type for kachery parameter?
    def download_results_if_needed(self, kachery:Union[str, None] = None) -> None:
        for f in _flatten_nested_collection(self._result, _type=File):
            assert isinstance(f, File), "Filter failed."
            f.ensure_local_availability(kachery)

    def download_parameter_files_if_needed(self, kachery:Union[str, None] = None) -> None:
        for a in _flatten_nested_collection(self._wrapped_function_arguments, _type=File):
            assert isinstance(a, File), "Filter failed."
            a.ensure_local_availability(kachery)

    def resolve_files_in_wrapped_arguments(self) -> None:
        """Handles file availability and unboxing of numpy arrays from Kachery files for
        items in the Job's wrapped function arguments.
        """
        # _replace_values replaces in-place for complex structures, but can't do so for
        # simple ones (e.g. if _wrapped_function_args is just a File). Have to reassign in that case.
        # The method is designed to modify the input in-place and also return it, for this reason.
        self._wrapped_function_arguments = \
            _replace_values_in_structure(self._wrapped_function_arguments,
                lambda r: r.resolve() if isinstance(r, File) else r)

    # TODO: Make this part of the .result() method? Would need to access info about
    # the "don't-resolve-results" parameter.
    def resolve_files_in_result(self) -> None:
        """Handles file availability and unboxing of numpy arrays from Kachery files for
        items in the Job's result.
        """
        self._result = _replace_values_in_structure(self._result,
            lambda r: r.resolve() if isinstance(r, File) else r)

    # TODO: What guarantee do we have that these are actually all complete? Should have a check for it
    def resolve_wrapped_job_values(self) -> None:
        self._wrapped_function_arguments = \
            _replace_values_in_structure(self._wrapped_function_arguments,
                lambda arg: arg.result() if isinstance(arg, Job) else arg)


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

    def needs_a_container_built(self) -> bool:
        """Returns whether this Job needs to have a container built before it can be run.

        Returns:
            bool -- Whether this Job requires a container to be built before it can be run.
        """
        if self._container is None: return False
        if self._job_handler.is_remote: return False
        # TODO: Check if the container has *already* been built?
        return True

    
    def _serialize(self, generate_code:bool):
        function_name = self._function_name
        function_version = self._function_version
        if generate_code:
            if self._code is not None:
                code = self._code
            else:
                assert self._f is not None, 'Cannot serialize function with generate_code=True when function and code are both not available'
                additional_files = getattr(self._f, '_hither_additional_files', [])
                local_modules = getattr(self._f, '_hither_local_modules', [])
                code = _generate_source_code_for_function(self._f, name=function_name, additional_files=additional_files, local_modules=local_modules)
            function = None
        else:
            assert self._f is not None, 'Cannot serialize function with generate_code=False when function is not available'
            code = None
            function = self._f
        x = dict(
            job_id=self._job_id,
            function=function,
            code=code,
            function_name=function_name,
            function_version=function_version,
            label=self._label,
            kwargs=_serialize_item(self._wrapped_function_arguments),
            container=self._container,
            download_results=self._download_results,
            job_timeout=self._job_timeout,
            no_resolve_input_files=self._no_resolve_input_files
        )
        x = _serialize_item(x, raise_exception=False)
        return x
    
    @staticmethod
    def _deserialize(serialized_job, job_manager=None):
        j = serialized_job
        return Job(
            f=j['function'],
            code=j['code'],
            function_name=j['function_name'],
            function_version=j['function_version'],
            label=j['label'],
            wrapped_function_arguments=_deserialize_item(j['kwargs']),
            container=j['container'],
            download_results=j.get('download_results', False),
            job_timeout=j.get('job_timeout', None),
            job_manager=job_manager,
            job_handler=None,
            job_cache=None,
            job_id=j['job_id'],
            no_resolve_input_files=j['no_resolve_input_files']
        )
