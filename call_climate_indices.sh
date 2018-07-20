#!/bin/bash

python --version
source activate indices_repo
python --version
python -c "import numpy; print(numpy.__version__)"
export NUMBA_DISABLE_JIT=1
python /g/data/u46/users/sc0554/climate_indices/scripts/process_grid_v2.py "$@"

