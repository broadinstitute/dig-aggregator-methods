#!/bin/bash -xe

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# Download all logs to be translated
sudo aws s3 cp s3://dig-analysis-data/out/ldsc/staging/genetic_correlation/ ./ --recursive
sudo rm ./*/_SUCCESS

# python packages
sudo yum install -y python3-devel
pip3 install -U Cython
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
