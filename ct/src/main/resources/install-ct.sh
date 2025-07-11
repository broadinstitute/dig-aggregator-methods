#!/bin/bash -xe

# PRScsx method
# Developed with python 3

ct_ROOT=/mnt/var/ct

# install to the root directory
sudo mkdir -p "$ct_ROOT"
cd "$ct_ROOT"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum update -y
sudo yum install -y jq
sudo yum install -y zstd


# install python dependencies
pip3 install -U pandas
pip3 install -U dask
pip3 install -U scipy
pip3 install -U numpy

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv

echo "Setup completed successfully. The 'C+T' environment is ready to use."  

