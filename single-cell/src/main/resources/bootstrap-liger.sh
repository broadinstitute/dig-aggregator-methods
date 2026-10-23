#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

sudo mkdir -p "$ROOT_DIR"
cd "$ROOT_DIR"

# Need to get specific R version the packages were compiled against
sudo yum groupinstall -y "Development Tools"
sudo yum install -y R-4.3.2-1.amzn2023.0.1

# This also acts as a way to freeze liger at a version w/ dependencies
sudo aws s3 cp s3://dig-analysis-bin/single_cell/liger_packages/latest/liger-packages.zip ./
sudo unzip -o liger-packages.zip -d /

# Needed for RcppPlanc
sudo yum -y install hwloc-devel

sudo wget https://github.com/HDFGroup/hdf5/releases/download/hdf5_1.14.4.3/hdf5-1.14.4-3.tar.gz
sudo tar zxvf hdf5-1.14.4-3.tar.gz
cd hdf5-1.14.4-3
sudo ./configure -prefix=/usr
sudo make -j -l6
sudo make install
cd ../
sudo rm hdf5-1.14.4-3.tar.gz
sudo rm -r hdf5-1.14.4-3

sudo aws s3 cp s3://dig-analysis-bin/single_cell/run_inmf.R ./

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
