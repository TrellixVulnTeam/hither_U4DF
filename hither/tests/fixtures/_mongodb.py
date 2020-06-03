import pytest
import os
import shutil
import hither as hi
from ._config import MONGO_PORT
from ._common import _random_string

@pytest.fixture()
def mongodb(tmp_path):
    print('Starting mongo database')
    with open(str(tmp_path / 'mongodb_out.txt'), 'w') as logf:
        dbpath = str(tmp_path / f'db-{_random_string(10)}')
        os.mkdir(dbpath)
        ss = hi.ShellScript(f"""
        #!/bin/bash
        set -ex

        exec mongod --dbpath {dbpath} --quiet --port {MONGO_PORT} --bind_ip localhost > /dev/null
        """)
        ss.start()
        yield ss
        print('Terminating mongo database')
        ss.stop()
        shutil.rmtree(dbpath)
