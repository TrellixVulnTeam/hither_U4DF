# pyright: reportMissingImports=false

import hither2 as hi

@hi.function('local_module', '0.1.0')
@hi.container('docker://jupyter/scipy-notebook:678ada768ab1')
@hi.local_modules(['./test_modules/test_module1'])
def local_module():
    import test_module1 
    assert test_module1.return42() == 42
    return True

local_module.test_calls = [
    dict(
        args=dict(),
        result=True,
        container_only=True
    )
]