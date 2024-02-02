#!/bin/bash -xe

LDSC_ROOT=/mnt/var/intake

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

# TODO: move to BIN_PATH and add to config as well
sudo aws s3 cp s3://dig-analysis-data/bin/qc/Homo_sapiens.GRCh37.75.dna.primary_assembly.fa ./
sudo aws s3 cp s3://dig-analysis-data/bin/qc/var_to_af.zip ./
sudo unzip var_to_af.zip -d ./g1000
sudo rm var_to_af.zip

# install dependencies
sudo yum install -y python3-devel
sudo yum install -y zstd
sudo pip3 install -U boto3
sudo pip3 install -U sqlalchemy
sudo pip3 install -U pymysql
sudo pip3 install -U Cython
sudo pip3 install -U pybind11
sudo pip3 install -U pythran
sudo pip3 install -U scipy
sudo pip3 install -U biopython
