import os
from copy import deepcopy
from .run_scriptdir_in_container import BindMount, DockerImage
from .run_function_in_container import run_function_in_container
from typing import Callable, Dict, List, Union

def run_function_in_container_with_kachery_support(
    function: Callable,
    image: DockerImage,
    kwargs: dict,
    modules: List[str] = [],
    environment: Dict[str, str] = dict(),
    bind_mounts: List[BindMount] = []
):
    import kachery_p2p as kp
    
    modules2 = deepcopy(modules)
    bind_mounts2 = deepcopy(bind_mounts)
    environment2 = deepcopy(environment)

    if 'kachery_p2p' not in modules2:
        modules2.append('kachery_p2p')
    
    kachery_storage_dir = kp._kachery_storage_dir()
    if kachery_storage_dir is None:
        raise Exception('Unable to determine kachery storage directory.')
    kachery_temp_dir = kp._kachery_temp_dir()
    bind_mounts2.append(
        BindMount(source=kachery_storage_dir, target=kachery_storage_dir, read_only=True)
    )
    bind_mounts2.append(
        BindMount(source=kachery_temp_dir, target=kachery_temp_dir, read_only=False)
    )
    environment2['KACHERY_TEMP_DIR'] = kachery_temp_dir
    kachery_p2p_api_port = os.getenv('KACHERY_P2P_API_PORT', None)
    if kachery_p2p_api_port is not None:
        environment2['KACHERY_P2P_API_PORT'] = kachery_p2p_api_port

    result_value = run_function_in_container(
        function=function,
        image=image,
        kwargs=kwargs,
        modules=modules2,
        environment=environment2,
        bind_mounts=bind_mounts2
    )

    return result_value