#!/bin/bash -xe

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# Download all logs to be translated
aws s3 cp s3://dig-analysis-data/out/ldsc/staging/genetic_correlation/ ./ --recursive
rm ./*/_SUCCESS

# python packages
sudo yum install -y python3-devel
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
