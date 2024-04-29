#!/bin/bash -xe

# python packages
sudo yum install -y python3-devel
pip3 install -U Cython
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
