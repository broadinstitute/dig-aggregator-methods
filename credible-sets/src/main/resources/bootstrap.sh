#!/bin/bash -xe

# install dependencies
sudo yum install -y python3-devel
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
