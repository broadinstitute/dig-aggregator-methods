#!/bin/bash -xe

## NOTE: This uses a python 3 version of ldsc which is saved as a zip in s3
## Developed with python 3.8.12 installed

LDSC_ROOT=/mnt/var/ldsc

# install to the root directory
sudo mkdir -p "$LDSC_ROOT"
cd "$LDSC_ROOT"

for ANCESTRY in AFR AMR EAS EUR SAS
do
  sudo mkdir -p ./weights/${ANCESTRY}
  sudo aws s3 cp s3://dig-ldsc-server/bin/weights/weights_${ANCESTRY}.zip .
  sudo unzip weights_${ANCESTRY}.zip -d ./weights/${ANCESTRY}/
  sudo rm weights_${ANCESTRY}.zip
done

# install dependencies
sudo yum install -y python3-devel

# reinstall numpy with openblas for multithreading
sudo yum -y install openblas-devel
pip3 uninstall -y numpy
pip3 install numpy

# install rest of python dependencies
sudo yum install -y zstd
pip3 install -U bitarray
pip3 install -U boto3
pip3 install -U sqlalchemy
pip3 install -U pymysql
pip3 install -U Cython
pip3 install -U numpy
pip3 install -U pybind11
pip3 install -U pythran
pip3 install -U scipy
pip3 install -U pandas

# fetch snps for mapping
sudo aws s3 cp s3://dig-ldsc-server/bin/snpmap/ ./snpmap/ --recursive
