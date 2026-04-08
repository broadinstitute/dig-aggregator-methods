#!/bin/bash -xe

ROOT_DIR=/mnt/var/single_cell

sudo mkdir -p "$ROOT_DIR"
cd "$ROOT_DIR"

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

sudo aws s3 cp s3://dig-analysis-bin/single_cell/inmf_liger_mod.R ./
