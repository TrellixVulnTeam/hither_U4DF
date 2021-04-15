import os
import hither2 as hi
import kachery_p2p as kp

# this image should be defined at the top level (outside the precontainer hook)
thisdir = os.path.dirname(os.path.realpath(__file__))
image = hi.DockerImageFromScript(dockerfile=f'{thisdir}/example_functions/docker/Dockerfile.numpy', name='magland/numpy')

class Hook1(hi.RuntimeHook):
    def __init__(self):
        super().__init__()
    def precontainer(self, context: hi.PreContainerContext):
        # this gets run outside the container before the run, and we have a chance to mutate the kwargs, add bind mounts, and set the image
        input_directory = context.kwargs['input_directory']
        context.kwargs['input_directory'] = '/input'
        context.add_bind_mount(hi.BindMount(source=input_directory, target='/input', read_only=True))
        context.image = image
    def postcontainer(self, context: hi.PostContainerContext):
        # this gets run outside the container after the run, and we have a chance to mutate the return value
        context.return_value = context.return_value + ['postcontainer-test']
    def prerun(self, context: hi.PreRunContext):
        # this gets run immediately before the function run, and we have a chance to mutate the kwargs
        context.kwargs['num'] = context.kwargs['num'] + 1
    def postrun(self, context: hi.PostRunContext):
        # this gets run immediately after the function run, and we have a chance to mutate the return value
        context.return_value = context.return_value + ['postrun-test']

@hi.function(
    'runtime_hook_example', '0.1.0',
    image=True, # True means that we will defer to setting the image to the precontainer hook
    runtime_hooks=[Hook1()]
)
def runtime_hook_example(input_directory: str, num: int):
    return [x for x in os.listdir(input_directory)] + [f'num={num}']

if __name__ == '__main__':
    # jh = hi.SlurmJobHandler(num_jobs_per_allocation=4, max_simultaneous_allocations=4, srun_command='')
    # jh = hi.ParallelJobHandler(num_workers=4)
    jh = None
    with hi.Config(use_container=True, show_console=True, job_handler=jh):
        with kp.TemporaryDirectory() as tmpdir:
            with open(f'{tmpdir}/testfile1.txt', 'w') as f:
                f.write('testcontent')
            j = hi.Job(runtime_hook_example, {'input_directory': tmpdir, 'num': 5})
            x = j.wait()
            print(x.return_value)
