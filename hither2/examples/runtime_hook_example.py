import os
import hither2 as hi
import kachery_p2p as kp

class Hook1(hi.RuntimeHook):
    def __init__(self):
        super().__init__()
    def precontainer(self, context: hi.PreContainerContext):
        # this gets run outside the container before the run, and we have a chance to mutate the kwargs and add bind mounts
        input_directory = context.kwargs['input_directory']
        context.kwargs['input_directory'] = '/input'
        context.add_bind_mount(hi.BindMount(source=input_directory, target='/input', read_only=True))
    def postcontainer(self, context: hi.PostContainerContext):
        # this gets run outside the container after the run, and we have a chance to mutate the return value
        context.return_value = context.return_value + ['postcontainer-test']
    def prerun(self, context: hi.PreRunContext):
        # this gets run immediately before the function run, and we have a chance to mutate the kwargs
        context.kwargs['num'] = context.kwargs['num'] + 1
    def postrun(self, context: hi.PostRunContext):
        # this gets run immediately after the function run, and we have a chance to mutate the return value
        context.return_value = context.return_value + ['postrun-test']
        pass

thisdir = os.path.dirname(os.path.realpath(__file__))

@hi.function(
    'runtime_hook_example', '0.1.0',
    image=hi.DockerImageFromScript(dockerfile=f'{thisdir}/example_functions/docker/Dockerfile.numpy', name='magland/numpy'),
    runtime_hooks=[Hook1()]
)
def runtime_hook_example(input_directory: str, num: int):
    return [x for x in os.listdir(input_directory)] + [f'num={num}']

if __name__ == '__main__':
    with hi.Config(use_container=True, show_console=True):
        with kp.TemporaryDirectory() as tmpdir:
            with open(f'{tmpdir}/testfile1.txt', 'w') as f:
                f.write('testcontent')
            j = hi.Job(runtime_hook_example, {'input_directory': tmpdir, 'num': 5})
            x = j.wait()
            print(x.return_value)
