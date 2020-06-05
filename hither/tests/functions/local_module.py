# pyright: reportMissingImports=false

import hither as hi

@hi.function('local_module', '0.1.0')
@hi.container('docker://jsoules/simplescipy:latest')
@hi.local_modules(['./test_modules/test_module1'])
def local_module():
    import test_module1 
    assert test_module1.return42() == 42
    return True

def test_calls():
    return [
        dict(
            args=dict(),
            result=True,
            container_only=True
        )
    ]

local_module.test_calls = test_calls