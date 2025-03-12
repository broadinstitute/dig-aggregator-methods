#!/bin/bash -xe

# PRScsx method
# Developed with python 3

prs_ROOT=/mnt/var/prs

# install to the root directory
sudo mkdir -p "$prs_ROOT"
cd "$prs_ROOT"

# install yum dependencies
sudo yum install -y python3-devel
sudo yum update -y
sudo yum install -y jq
sudo yum install -y zstd

# Install parallel
# sudo yum -y install epel-release
sudo amazon-linux-extras install epel -y
sudo yum install -y parallel

# pull down LD bfiles
sudo mkdir -p ./bfiles
sudo aws s3 cp s3://dig-analysis-bin/cojo/bfiles/ ./bfiles/ --recursive

# pull down prs dir
sudo mkdir -p ./prscsx
sudo aws s3 cp s3://dig-analysis-bin/prs/prscsx/ ./prscsx/ --recursive

sudo mkdir -p ./ref_info
sudo aws s3 cp s3://dig-analysis-bin/prs/ref_info/ ./ref_info/ --recursive

# fetch snps for mapping
sudo aws s3 cp "s3://dig-analysis-bin/snps/dbSNP_common_GRCh37.csv" ./snps.csv


# install python dependencies
pip3 install -U pandas
pip3 install -U dask
pip3 install -U gcsfs
pip3 install -U scipy
pip3 install -U numpy
pip3 install -U pyspark
pip3 install -U jq
pip3 install -U h5py
pip3 install -U math

# Install JRE
sudo yum install -y java-1.8.0-openjdk-devel

echo "Setup completed successfully. The 'PRScsx' environment is ready to use."  
