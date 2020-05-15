# import os
# import time
# import json
# import hither as hi
# import pytest
# import numpy as np
# import kachery as ka
# from .functions import functions as fun
# from .fixtures import MONGO_PORT, DATABASE_NAME, COMPUTE_RESOURCE_ID, KACHERY_CONFIG

# def _run_pipeline(*, delay=None, shape=(6, 3)):
#     f = fun.zeros.run(shape=(6, 3))
#     g = fun.add.run(x=f, y=1)
#     with hi.Config(download_results=True):
#         A = fun.identity.run(x=g)
#     A.wait(0.1) # For code coverage
#     a = A.wait().array()
#     print('===========================================================')
#     print(a)
#     print('===========================================================')
#     assert a.shape == shape
#     assert np.allclose(a, np.ones(shape))

# def _run_short_pipeline(*, delay=None, shape=(6, 3)):
#     f = fun.zeros.run(shape=(6, 3))
#     with hi.Config(download_results=True):
#         A = fun.identity.run(x=f)
#     A.wait(0.1) # For code coverage
#     a = A.wait().array()
#     assert a.shape == shape
#     assert np.allclose(a, np.zeros(shape))

# def test_1(general, mongodb):
#     _run_pipeline()
#     with hi.ConsoleCapture(label='[test_1]') as cc:
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         jc = hi.JobCache(database=db)
#         with hi.Config(container=True, job_cache=jc):
#             for num in range(2):
#                 timer = time.time()
#                 _run_pipeline()
#                 elapsed = time.time() - timer
#                 print(f'Elapsed for pass {num}: {elapsed}')
#                 if num == 1:
#                     assert elapsed < 2
#         cc.runtime_info() # for code coverage

# @pytest.mark.focus
# @pytest.mark.compute_resource
# def test_2(general, compute_resource, mongodb, kachery):
#     with hi.ConsoleCapture(label='[test_2]'):
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
#         with hi.Config(job_handler=rjh, container=True):
#             for num in range(2):
#                 timer = time.time()
#                 _run_short_pipeline(delay=1)
#                 elapsed = time.time() - timer
#                 print(f'Elapsed for pass {num}: {elapsed}')
#                 if num == 1:
#                     assert elapsed < 2
#             with hi.Config(download_results=True):
#                 _run_pipeline(shape=(6, 3))
#         hi.wait() # for code coverage

# def test_file_lock(general, tmp_path):
#     # For code coverage
#     with hi.ConsoleCapture(label='[test_file_lock]'):
#         path = str(tmp_path)
#         with hi.FileLock(path + '/testfile.txt', exclusive=False):
#             pass
#         with hi.FileLock(path + '/testfile.txt', exclusive=True):
#             pass

# def test_misc(general):
#     # For code coverage
#     import pytest
#     with hi.ConsoleCapture(label='[test_misc]'):
#         f = fun.zeros.run(shape=(3, 4), delay=0)
#         with pytest.raises(Exception):
#             f.result()
#         f.wait()
#         f.result()

# @pytest.mark.compute_resource
# def test_job_error(general, compute_resource, mongodb, kachery):
#     import pytest
    
#     with hi.ConsoleCapture(label='[test_job_error]'):
#         x = fun.intentional_error.run()
#         with pytest.raises(Exception):
#             a = x.wait()
#         assert str(x.exception()) == 'intentional-error'

#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
#         with hi.Config(job_handler=rjh, container=True):
#             for _ in range(2):
#                 x = fun.intentional_error.run()
#                 with pytest.raises(Exception):
#                     a = x.wait()
#                 assert str(x.exception()) == 'intentional-error'
#         jc = hi.JobCache(database=db, cache_failing=True)
#         with hi.Config(job_cache=jc, container=True):
#             for _ in range(2):
#                 x = fun.intentional_error.run()
#                 with pytest.raises(Exception):
#                     a = x.wait()
#                 assert str(x.exception()) == 'intentional-error'

# @pytest.mark.compute_resource
# def test_bad_container(general, compute_resource, mongodb, kachery):
#     import pytest
    
#     with hi.ConsoleCapture(label='[test_bad_container]'):
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)

#         fun.bad_container.run().wait()

#         with hi.Config(container=True):
#             x = fun.bad_container.run()
#             with pytest.raises(Exception):
#                 x.wait()
        
#         with hi.Config(job_handler=rjh, container=True):
#             x = fun.bad_container.run()
#             with pytest.raises(Exception):
#                 x.wait()

# @pytest.mark.compute_resource
# def test_job_arg_error(general, compute_resource, mongodb, kachery):
#     import pytest
    
#     with hi.ConsoleCapture(label='[test_job_arg_error]'):
#         x = fun.intentional_error.run()
#         a = fun.do_nothing.run(x=x)
#         with pytest.raises(Exception):
#             a.wait()

# def test_wait(general):
#     pjh = hi.ParallelJobHandler(num_workers=4)
#     with hi.Config(job_handler=pjh):
#         a = fun.do_nothing.run(x=None, delay=0.2)
#         hi.wait(0.1)
#         hi.wait()
#         assert a.result() == None

# def test_extras(general):
#     with hi.Config(container='docker://jupyter/scipy-notebook:678ada768ab1'):
#         a = fun.additional_file.run()
#         assert a.wait().array().shape == (2, 3)

#         a = fun.local_module.run()
#         assert a.wait() == True

# @pytest.mark.compute_resource
# def test_missing_input_file(general, compute_resource, mongodb, kachery):
#     with hi.ConsoleCapture(label='[test_missing_input_file]'):
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
#         path = ka.store_text('test-text')
#         false_path = path.replace('0', '1')
#         assert path != false_path

#         with hi.Config(container=True):
#             a = fun.do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-1')
#             a.wait()
#             b = fun.do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-2')
#             with pytest.raises(Exception):
#                 b.wait()
        
#         with hi.Config(job_handler=rjh, container=True):
#             a = fun.do_nothing.run(x=[dict(some_file=hi.File(path))]).set(label='do-nothing-remotely-1')
#             a.wait()
#             b = fun.do_nothing.run(x=[dict(some_file=hi.File(false_path))]).set(label='do-nothing-remotely-2')
#             with pytest.raises(Exception):
#                 b.wait()

# @pytest.mark.focus
# @pytest.mark.compute_resource
# def test_identity(general, compute_resource, mongodb, kachery):
#     with hi.ConsoleCapture(label='[test_identity]'):
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
#         path = ka.store_text('test-text-2')

#         with hi.Config(container=True):
#             a = ([dict(file=hi.File(path))],)
#             b = fun.identity.run(x=a).wait()
#             assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)
        
#         with hi.Config(job_handler=rjh, container=True, download_results=True):
#             a = ([dict(file=hi.File(path))],)
#             b = fun.identity.run(x=a).wait()
#             assert ka.get_file_hash(b[0][0]['file'].path) == ka.get_file_hash(path)

# def test_slurm_job_handler(general, tmp_path):
#     slurm_working_dir = str(tmp_path / 'slurm-job-handler')
#     sjh = hi.SlurmJobHandler(
#         working_dir=slurm_working_dir,
#         use_slurm=False,
#         num_workers_per_batch=3,
#         num_cores_per_job=2,
#         time_limit_per_batch=2400,  # number of seconds or None
#         max_simultaneous_batches=5,
#         additional_srun_opts=[]
#     )
#     with hi.ConsoleCapture(label='[slurm_job_handler]'):
#         with hi.Config(container=True, job_handler=sjh):
#             shapes = [(j, 3) for j in range(1, 8)]
#             results = []
#             for shape in shapes:
#                 f = fun.zeros(shape=shape, delay=0.1)
#                 g = fun.add.run(x=f, y=1)
#                 A = fun.identity.run(x=g)
#                 results.append(A)
#             for i in range(len(shapes)):
#                 assert shapes[i] == results[i].wait().array().shape
#                 print(f'Checked: {shapes[i]} {results[i].wait().array().shape}')

# @pytest.mark.compute_resource
# def test_combo_local_remote(general, compute_resource, mongodb, kachery):
#     with hi.ConsoleCapture(label='[test_combo_local_remote]'):
#         db = hi.Database(mongo_url=f'mongodb://localhost:{MONGO_PORT}', database=DATABASE_NAME)
#         rjh = hi.RemoteJobHandler(database=db, compute_resource_id=COMPUTE_RESOURCE_ID)
        
#         A = np.ones((5, 5))

#         with hi.Config(container=True, job_handler=rjh):
#             with hi.Config(download_results=True):
#                 B = fun.add.run(x=A, y=1)
#         b = B.wait().array()
#         assert np.allclose(A + 1, b)

# # def test_spikeforest_remote_compute_resource(general, remote_compute_resource):
# def test_spikeforest_remote_compute_resource(general):
#     if not os.getenv('SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD', None):
#         print('Skipping remote compute resource test because environment variable not set: SPIKEFOREST_COMPUTE_RESOURCE_READWRITE_PASSWORD')
#         return

#     db = hi.Database.preset('spikeforest_readwrite')
#     rjh = hi.RemoteJobHandler(database=db, compute_resource_id='spikeforest1')
#     with hi.Config(container=True, job_handler=rjh):
#         _run_pipeline()

# def test_preset_config(general):
#     db = hi.Database.preset('spikeforest_readonly')
#     assert db is not None

# def test_bin(general, tmp_path, mongodb, kachery):
#     working_dir = str(tmp_path / 'compute-resource')
#     os.mkdir(working_dir)
#     ss1 = hi.ShellScript(f"""
#     #!/bin/bash
#     set -ex

#     cd {working_dir}
#     hither-compute-resource init
#     """)
#     ss1.start()
#     ss1.wait()
#     config_fname = working_dir + '/compute_resource.json'
#     with open(config_fname, 'r') as f:
#         config = json.load(f)
#     config['compute_resource_id'] = 'test_resource_1'
#     database_config = dict(
#         mongo_url = f'mongodb://localhost:{MONGO_PORT}',
#         database='test_database'
#     )
#     config['database'] = database_config
#     config['kachery'] = KACHERY_CONFIG
#     with open(config_fname, 'w') as f:
#         json.dump(config, f)
#     ss2 = hi.ShellScript(f"""
#     #!/bin/bash
#     set -ex

#     cd {working_dir}
#     hither-compute-resource start
#     """)
#     ss2.start()
#     ss2.wait(1)

#     db = hi.Database(**database_config)
#     rjh = hi.RemoteJobHandler(
#         database=db,
#         compute_resource_id='test_resource_1',
#     )
#     with hi.Config(container=True, job_handler=rjh):
#         _run_short_pipeline()
#         hi.wait()
#     ss2.kill()