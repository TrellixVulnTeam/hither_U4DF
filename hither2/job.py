import os
import sys
import time
from typing import Union, Any
from copy import deepcopy

import kachery as ka
from ._config import config
from ._enums import JobStatus
from .file import File
from ._generate_source_code_for_function import _generate_source_code_for_function
from ._resolve_files_in_item import _resolve_files_in_item, _deresolve_files_in_item
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
        self._wrapped_function_arguments = _deresolve_files_in_item(wrapped_function_arguments)
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
                if self._status in JobStatus.get_prerun_statuses():
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
                        with config(job_handler=self._job_handler, download_results=True):
                            self._substitute_job_for_wait = identity.run(x=result)
                        # compute the remainder timeout for this call to wait()
                        timeout2 = timeout
                        if timeout2 is not None:
                            elapsed = time.time() - timer
                            timeout2 = max(0, timeout2 - elapsed)
                        return self._substitute_job_for_wait.wait(timeout=timeout2, resolve_files=resolve_files)
                    else:
                        return _resolve_files_in_item(self._result)
        while True:
            self._job_manager.iterate()
            if self._status == JobStatus.FINISHED:
                if resolve_files:
                    return _resolve_files_in_item(self._result)
                else:
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

    def status(self):
        return self._status

    def has_been_submitted(self) -> bool:
        return not self._status in JobStatus.get_prerun_statuses()

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
        result_items = _flatten_nested_collection(actual_result)
        for item in result_items:
            if not isinstance(item, File):
                continue
            info = ka.get_file_info(item._sha1_path, fr=None)
            if info is None:
                return False
        return True

    def _ensure_job_results_available_locally(self, job: Any) -> Any:
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
            with config(job_handler=job._job_handler, download_results=True, container=True):
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
        _replace_values_in_structure(self._wrapped_function_arguments, self._ensure_job_results_available_locally)

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
                    kwargs = _resolve_files_in_item(self._wrapped_function_arguments)
                else:
                    kwargs = self._wrapped_function_arguments
                ret = self._f(**kwargs)
                self._result = _deresolve_files_in_item(ret)
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
    
    def _serialize(self, generate_code):
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
        x = _serialize_item(x)
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
