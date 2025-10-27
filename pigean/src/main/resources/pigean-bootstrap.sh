#!/bin/bash -xe

PIGEAN_ROOT=/mnt/var/pigean

# create a directory in /mnt/var to copy data locally
sudo mkdir -p "${PIGEAN_ROOT}"
sudo chmod 775 "${PIGEAN_ROOT}"

# install to the metal directory
cd "${PIGEAN_ROOT}"

sudo aws s3 cp s3://dig-analysis-data/out/pigean/gene_lists/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/gene_sets/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/methods/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/misc/ . --recursive
sudo aws s3 cp s3://dig-analysis-bin/pigean/models/ . --recursive

sudo aws s3 cp s3://dig-analysis-bin/orphanet/code_to_leaves.tsv .

# install dependencies
sudo yum install -y python3-devel
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
sudo pip3 install -U requests
sudo pip3 install -U networkx
sudo pip3 install -U matplotlib
