import pytest
import sys
import os

thisdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(thisdir)

pytest_plugins = [
    "fixtures._general"
]   

def pytest_addoption(parser):
    parser.addoption('--container', action='store_true', dest="container",
                 default=False, help="enable container tests")

def pytest_configure(config):
    config.addinivalue_line(
        "markers", "container: test runs jobs in a container"
    )
    if not config.option.container:
        setattr(config.option, 'markexpr', 'not container')