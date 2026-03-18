#!/bin/bash -xe

sudo yum install -y python3-devel
sudo pip3 install pandas --no-deps
sudo pip3 install Cython
sudo pip3 install pybind11
sudo pip3 install pythran
sudo pip3 install scipy

# anndata attempts to overwrite python-datetime which corrupts aws-cli (via boto3)
# The above pandas install and the other manual installs of dependencies heads off this issue
sudo pip3 install anndata --no-deps
sudo pip3 install h5py
sudo pip3 install packaging
sudo pip3 install exceptiongroup
sudo pip3 install natsort
sudo pip3 install array-api-compat
sudo pip3 install pytz
