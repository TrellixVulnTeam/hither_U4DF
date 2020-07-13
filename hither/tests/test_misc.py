import os
import pytest
import hither as hi

def test_misc():
    with pytest.raises(hi.DuplicateFunctionException):
        from .doubly_defined import doubly_defined_1, doubly_defined_2
