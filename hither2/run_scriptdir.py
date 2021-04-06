import time
from .scriptdir_runner import ScriptDirRunnerJob

from numpy import source
from .dockerimage import DockerImage, RemoteDockerImage

def run_scriptdir(*,
    scriptdir: str
):
    j = ScriptDirRunnerJob(scriptdir)
    j.start()
    while True:
        j.iterate()
        if j.status == 'complete':
            break
        time.sleep(0.1)
    return j