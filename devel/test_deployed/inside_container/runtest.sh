#!/bin/bash

# This file gets copied into the container
echo $KACHERY_STORAGE_DIR

# First run the basic tests without changing directories
pytest --pyargs hither -s

# Now change to the module directory and run full tests
# (there must be an easier line to get the module location)
cd `python -c "import os, hither; print(os.path.dirname(hither.__file__))"`
pytest --pyargs hither -s --container --remote
