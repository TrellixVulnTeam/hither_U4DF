import pytest
import sys
import os

thisdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(thisdir)

pytest_plugins = [
    "fixtures._general",
    "fixtures._compute_resource",
    "fixtures._kachery_p2p_daemon"
]

def pytest_addoption(parser):
    parser.addoption('--container', action='store_true', dest="container",
                 default=False, help="enable container tests")
    parser.addoption('--remote', action='store_true', dest="remote",
                 default=False, help="enable remote tests")
    parser.addoption('--current', action='store_true', dest="current",
                 default=False, help="run only tests marked as current")

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "container: test runs jobs in a container"
    )
    config.addinivalue_line(
        "markers", "remote: test runs jobs on remote server"
    )
    config.addinivalue_line(
        "markers", "current: for convenience -- mark one test as current"
    )

    markexpr_list = []
    
    if config.option.current:
        markexpr_list.append('current')
    else:
        if not config.option.container:
            markexpr_list.append('not container')
        
        if not config.option.remote:
            markexpr_list.append('not remote')
    
    if len(markexpr_list) > 0:
        markexpr = ' and '.join(markexpr_list)
        setattr(config.option, 'markexpr', markexpr)