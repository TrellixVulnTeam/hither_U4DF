import pytest
import sys
import os

thisdir = os.path.dirname(os.path.realpath(__file__))
sys.path.append(thisdir)

pytest_plugins = [
    "fixtures._general"
]