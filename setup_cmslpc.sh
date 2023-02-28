#!/bin/env bash

#### Use python 3.8.6
#source /cvmfs/sft.cern.ch/lcg/views/LCG_100/x86_64-centos7-gcc8-opt/setup.sh
#export PYTHONPATH=~/.local/lib/python3.8/site-packages:$PYTHONPATH

#### Use python 3.9.12
source /cvmfs/sft.cern.ch/lcg/views/LCG_103/x86_64-centos7-gcc11-opt/setup.sh
export PYTHONPATH=~/.local/lib/python3.9/site-packages:$PYTHONPATH

#python -m pip install lip-pps-run-manager --user
#python -m pip install plotly --user  # Prefer this one, but use the one below if the wrong version is installed
#python -m pip install plotly==5.13.1 --user
#python -m pip install pandas --user
#python -m pip install pyarrow --user
#python -m pip install statsmodels --user
#python -m pip install sympy --user