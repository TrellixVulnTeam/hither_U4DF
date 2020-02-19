import random

def _docker_form_of_container_string(container):
    if container.startswith('docker://'):
        return container[len('docker://'):]
    else:
        return container

def _random_string(num: int):
    """Generate random string of a given length.
    """
    return ''.join(random.choices('abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ', k=num))