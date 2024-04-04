#!/bin/bash -xe

# install needed python
sudo yum install -y python3-devel
pip3 install -U boto3
pip3 install -U Cython
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy

# download NCBI data
MAGMA_DIR="/mnt/var/magma"
sudo mkdir -p "${MAGMA_DIR}"
cd "${MAGMA_DIR}"
sudo aws s3 cp s3://dig-analysis-bin/magma/NCBI37.3.gene.loc .
