#!/bin/bash -xe

# install needed python
sudo yum install -y python3-devel
sudo pip3 install -U boto3
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy

# download NCBI data
MAGMA_DIR="/mnt/var/magma"
mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"
aws s3 cp s3://dig-analysis-data/bin/magma/NCBI37.3.gene.loc .
